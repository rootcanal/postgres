/*------------------------------------------------------------------------
 * vertical.c
 *		Simple column store implementation for POSTGRES
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * src/backend/colstore/vertical.c
 *
 *------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/heap.h"
#include "colstore/colstoreapi.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

/* XXX figure out about scanDesc */
#include "access/relscan.h"


typedef struct VerticalState
{
	uint32		magic;
	HeapScanDesc scanDesc;
	BulkInsertState	bistate;
} VerticalState;

#define VerticalColstoreMagic 0xC522C8AA

static void vertical_open(ColumnStoreHandler *handler,
						   bool for_write, Snapshot snapshot);
static void vertical_insert(ColumnStoreHandler *handler, Datum *values,
				bool *nulls, CommandId cid);
static void vertical_batch_insert(ColumnStoreHandler *handler, int nrows,
					  Datum **values, bool **nulls, CommandId cid);
static TupleTableSlot *vertical_scannext(ColumnStoreHandler *handler,
				  TupleTableSlot *slot);
static void vertical_endscan(ColumnStoreHandler *handler);
static void vertical_rescan(ColumnStoreHandler *handler);
static void vertical_markpos(ColumnStoreHandler *handler);
static void vertical_restrpos(ColumnStoreHandler *handler);
static void vertical_close(ColumnStoreHandler *handler);
static void vertical_truncate(Relation rel);

Datum
vertical_cstore_handler(PG_FUNCTION_ARGS)
{
	ColumnStoreRoutine	   *routine = makeNode(ColumnStoreRoutine);

	routine->ExecColumnStoreOpen = vertical_open;
	routine->ExecColumnStoreInsert = vertical_insert;
	routine->ExecColumnStoreBatchInsert = vertical_batch_insert;
	routine->ExecColumnStoreScanNext = vertical_scannext;
	routine->ExecColumnStoreEndScan = vertical_endscan;
	routine->ExecColumnStoreRescan = vertical_rescan;
	routine->ExecColumnStoreMarkPos = vertical_markpos;
	routine->ExecColumnStoreRestrPos = vertical_restrpos;
	routine->ExecColumnStoreClose = vertical_close;
	routine->ExecColumnStoreTruncate = vertical_truncate;

	PG_RETURN_POINTER(routine);
}

/*
 * Fill in the ColumnStoreHandler opaque pointer, either in read-only mode or
 * write-only, so that other routines can be invoked.
 *
 * Note that the colstore can opened for either read or write; a single handler
 * can not do both.  XXX this seems a pointless limitation.
 *
 * If opened for read, caller must supply a snapshot.
 */
static void
vertical_open(ColumnStoreHandler *handler, bool for_write, Snapshot snapshot)
{
	VerticalState  *state = (VerticalState *) palloc0(sizeof(VerticalState));

	state->magic = VerticalColstoreMagic;
	if (for_write)
		state->bistate = GetBulkInsertState();
	else
		state->scanDesc =
			heap_beginscan(handler->csh_relationDesc, snapshot, 0, NULL);

	handler->csh_opaque = (void *) state;
}

/*
 * Insert values for a single tuple, marked with the given command ID.
 *
 * XXX Note that the ItemPointer must be the first element in the values/isnull
 * arrays.  This is an ugly crock which must be removed.
 */
static void
vertical_insert(ColumnStoreHandler *handler, Datum *values, bool *isnull,
				CommandId cid)
{
	VerticalState  *state = (VerticalState *) handler->csh_opaque;
	HeapTuple	tuple;
	TupleDesc	tupdesc;

	if (state->magic != VerticalColstoreMagic)
		ereport(ERROR,
				(errmsg("unsightly pointer")));
	Assert(state->bistate);

	tupdesc = RelationGetDescr(handler->csh_relationDesc);
	tuple = heap_form_tuple(tupdesc, values, isnull);

	heap_insert(handler->csh_relationDesc, tuple, cid,
				0 /* XXX no options --- OK? */,
				state->bistate);
	heap_freetuple(tuple);
}

/*
 * Insert values for multiple tuples, as above.
 */
static void
vertical_batch_insert(ColumnStoreHandler *handler, int nrows, Datum **values,
					  bool **isnull, CommandId cid)
{
	VerticalState *state = (VerticalState *) handler->csh_opaque;
	int			i;
	TupleDesc	tupdesc;

	if (state->magic != VerticalColstoreMagic)
		ereport(ERROR,
				(errmsg("unsightly pointer")));
	Assert(state->bistate);
	tupdesc = RelationGetDescr(handler->csh_relationDesc);

	for (i = 0; i < nrows; i++)
	{
		HeapTuple	tuple;

		tuple = heap_form_tuple(tupdesc, values[i], isnull[i]);

		heap_insert(handler->csh_relationDesc, tuple, cid,
					0, state->bistate);
		heap_freetuple(tuple);
	}
}

static TupleTableSlot *
vertical_scannext(ColumnStoreHandler *handler, TupleTableSlot *slot)
{
	VerticalState  *state = (VerticalState *) handler->csh_opaque;
	HeapTuple		tuple;

	if (state->magic != VerticalColstoreMagic)
		ereport(ERROR,
				(errmsg("unsightly pointer")));
	Assert(state->scanDesc);

	tuple = heap_getnext(state->scanDesc, ForwardScanDirection);
	if (tuple)
		ExecStoreTuple(tuple,
					   slot,
					   state->scanDesc->rs_cbuf,
					   false);
	else
		ExecClearTuple(slot);

	return slot;
}

static void
vertical_endscan(ColumnStoreHandler *handler)
{
	VerticalState  *state = (VerticalState *) handler->csh_opaque;

	if (state->magic != VerticalColstoreMagic)
		ereport(ERROR,
				(errmsg("unsightly pointer")));
	Assert(state->scanDesc);

	/*
	 * close heap scan
	 */
	heap_endscan(state->scanDesc);
}

static void
vertical_rescan(ColumnStoreHandler *handler)
{
	VerticalState  *state = (VerticalState *) handler->csh_opaque;
	HeapScanDesc scan;


	if (state->magic != VerticalColstoreMagic)
		ereport(ERROR,
				(errmsg("unsightly pointer")));
	Assert(state->scanDesc);

	scan = state->scanDesc;

	heap_rescan(scan,			/* scan desc */
				NULL);			/* new scan keys */
}

static void
vertical_markpos(ColumnStoreHandler *handler)
{

}

static void
vertical_restrpos(ColumnStoreHandler *handler)
{

}

/*
 * Release resources
 */
static void
vertical_close(ColumnStoreHandler *handler)
{
	VerticalState  *state = (VerticalState *) handler->csh_opaque;

	if (state->magic != VerticalColstoreMagic)
		ereport(ERROR,
				(errmsg("unsightly pointer")));

	/* Release all resources */
	if (state->scanDesc)
		heap_endscan(state->scanDesc);
	FreeBulkInsertState(state->bistate);
	pfree(state);
}

static void
vertical_truncate(Relation rel)
{
	heap_truncate_one_rel(rel);
}
