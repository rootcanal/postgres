/*-------------------------------------------------------------------------
 *
 * execColumnStore.c
 *	  routines for inserting tuples into column stores.
 *
 * ExecInsertColStoreTuples() is the main entry point.  It's called after
 * inserting a tuple to the heap, and it inserts corresponding values
 * into all column stores.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execColumnStore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "catalog/colstore.h"
#include "colstore/colstoreapi.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/tqual.h"


/* ----------------------------------------------------------------
 *		ExecOpenColumnStores
 *
 *		Find the column stores associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
void
ExecOpenColumnStores(ResultRelInfo *resultRelInfo)
{
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;
	List	   *colstoreoidlist;
	ListCell   *l;

	/* fast path if no column stores */
	if (!RelationGetForm(resultRelation)->relhascstore)
	{
		resultRelInfo->ri_ColumnStoreHandler = NIL;
		return;
	}

	/*
	 * Get cached list of colstore OIDs; bail out if there are no colstores
	 * after all.
	 */
	colstoreoidlist = RelationGetColStoreList(resultRelation);
	if (colstoreoidlist == NIL)
	{
		resultRelInfo->ri_ColumnStoreHandler = NIL;
		return;
	}

	/* Open each colstore and stash it into the list */
	foreach(l, colstoreoidlist)
	{
		Oid			cstoreOid = lfirst_oid(l);
		Relation	cstoreDesc;
		ColumnStoreHandler *handler;
		LOCKMODE	lockmode = RowExclusiveLock;

		cstoreDesc = relation_open(cstoreOid, lockmode);

		/* Build the column store handler */
		handler = BuildColumnStoreHandler(cstoreDesc, true, lockmode, NULL);

		/* add it to the list */
		resultRelInfo->ri_ColumnStoreHandler =
			lappend(resultRelInfo->ri_ColumnStoreHandler, handler);
	}

	list_free(colstoreoidlist);
}

/* ----------------------------------------------------------------
 *		ExecCloseColumnStores
 *
 *		Close the column store relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
void
ExecCloseColumnStores(ResultRelInfo *resultRelInfo)
{
	ListCell   *l;

	foreach (l, resultRelInfo->ri_ColumnStoreHandler)
	{
		ColumnStoreHandler *handler = lfirst(l);

		CloseColumnStore(handler);
	}
	/*
	 * resetting the list to NIL avoids keeping dangling pointers around, but
	 * risks confusing code into thinking this relation has no column stores.
	 * Therefore we set the list to an invalid pointer instead.
	 */
	resultRelInfo->ri_ColumnStoreHandler = (List *) 0x7f;
}

/* ----------------------------------------------------------------
 *		ExecInsertColStoreTuples
 *
 *		This routine takes care of inserting column store tuples
 *		into all the relations vertically partitioning the result relation
 *		when a heap tuple is inserted into the result relation.
 *
 *		CAUTION: this must not be called for a HOT update.
 *		We can't defend against that here for lack of info.
 *		Should we change the API to make it safer?
 * ----------------------------------------------------------------
 */
void
ExecInsertColStoreTuples(HeapTuple tuple, EState *estate)
{
	ResultRelInfo *resultRelInfo;
	Datum		values[INDEX_MAX_KEYS];	/* FIXME INDEX_MAX_KEYS=32 seems a bit low */
	bool		isnull[INDEX_MAX_KEYS];
	TupleDesc	tupdesc;
	ListCell   *l;

	/*
	 * Get information from the result relation info structure.
	 */
	resultRelInfo = estate->es_result_relation_info;
	tupdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

	foreach(l, resultRelInfo->ri_ColumnStoreHandler)
	{
		ColumnStoreHandler *handler = lfirst(l);
		ExecColumnStoreInsert_function insert;

		insert = handler->csh_ColumnStoreRoutine->ExecColumnStoreInsert;
		if (insert == NULL)
			elog(ERROR, "Insert routine not supplied by column store AM");

		/* Obtain the data arrays for the column store */
		FormColumnStoreDatum(handler, tuple, tupdesc, values, isnull);

		/* And insert them */
		insert(handler, values, isnull, estate->es_output_cid);
	}
}

/* same thing, but for N tuples.  XXX comment some more */
void
ExecBatchInsertColStoreTuples(int ntuples, HeapTuple *tuples, EState *estate)
{
	ResultRelInfo *resultRelInfo;
	TupleDesc	tupdesc;
	ListCell   *l;
	Datum	  **lvalues;
	bool	  **lisnull;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;

	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "BatchInsertColStore",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	/*
	 * Get information from the result relation info structure.
	 */
	resultRelInfo = estate->es_result_relation_info;
	tupdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

	Assert(tupdesc->tdattorder == ATTRORDER_OFFHEAPATTRS);

	/*
	 * Allocate a pointer for each tuple. This is required by the
	 * ExecColumnStoreBatchInsert API function
	 */
	lvalues = (Datum **) palloc(sizeof(Datum *) * ntuples);
	lisnull = (bool **) palloc(sizeof(bool *) * ntuples);

	foreach (l, resultRelInfo->ri_ColumnStoreHandler)
	{
		ColumnStoreHandler *handler = (ColumnStoreHandler *) lfirst(l);
		ExecColumnStoreBatchInsert_function batchInsert;
		Datum	   *values;
		bool	   *isnull;
		int			cstore_natts;
		int			tup;

		batchInsert = handler->csh_ColumnStoreRoutine->ExecColumnStoreBatchInsert;

		if (batchInsert == NULL)
			elog(ERROR, "BatchInsert routine not supplied by column store AM");

		/* The column store has an extra column for the heaptid */
		cstore_natts = handler->csh_NumColumnStoreAttrs + 1;

		/*
		 * To save performing a palloc once for each tuple, we'll just allocate
		 * all in one giant block.
		 */
		values = (Datum *) palloc(sizeof(Datum *) * cstore_natts * ntuples);
		isnull = (bool *) palloc(sizeof(bool *) * cstore_natts * ntuples);

		for (tup = 0; tup < ntuples; tup++)
		{
			int i;

			/*
			 * set lvalues and lisnull to point to the first attribute of each
			 * tuple.
			 */
			lvalues[tup] = &values[tup * cstore_natts];
			lisnull[tup] = &isnull[tup * cstore_natts];

			/* Assign the ctid of the heap to the colstore's heaptid column */
			lvalues[tup][0] = PointerGetDatum(&tuples[tup]->t_self);
			lisnull[tup][0] = false;

			for (i = 1; i < cstore_natts; i++)
			{
				AttrNumber attnum = handler->csh_KeyAttrNumbers[i - 1];
				lvalues[tup][i] = heap_getlogattr(tuples[tup], attnum, tupdesc, &lisnull[tup][i]);
			}
		}

		/* finally call the API function to store the tuples */
		batchInsert(handler, ntuples, lvalues, lisnull, estate->es_output_cid);

		pfree(values);
		pfree(isnull);
	}

	pfree(lvalues);
	pfree(lisnull);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);
}

void
ExecTruncateColumnStores(Relation rel)
{
	ListCell   *l;
	List	   *cstore_oids = RelationGetColStoreList(rel);

	foreach(l, cstore_oids)
	{
		Oid oid = lfirst_oid(l);
		Relation colstorerel = heap_open(oid, AccessExclusiveLock);
		ColumnStoreRoutine *routine;
		ExecColumnStoreTruncate_function csttruncate;

		routine = GetColumnStoreRoutineForRelation(colstorerel, false);

		csttruncate = routine->ExecColumnStoreTruncate;
		if (csttruncate == NULL)
			elog(ERROR, "Truncate routine not provided by column store AM");

		csttruncate(colstorerel);

		heap_close(colstorerel, AccessExclusiveLock);
	}
}
