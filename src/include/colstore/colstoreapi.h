/*-------------------------------------------------------------------------
 *
 * colstoreapi.h
 *	  API for column store implementations
 *
 * Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 * src/include/colstore/colstoreapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COLSTOREAPI_H
#define COLSTOREAPI_H

#include "nodes/execnodes.h"
#include "nodes/relation.h"

typedef void (*ExecColumnStoreOpen_function) (ColumnStoreHandler *handler,
				bool for_write, Snapshot snapshot);

typedef void (*ExecColumnStoreInsert_function) (ColumnStoreHandler *handler,
				Datum *values, bool *nulls, CommandId cid);

typedef void (*ExecColumnStoreBatchInsert_function) (ColumnStoreHandler *handler,
				int nrows, Datum **values, bool **nulls,
				CommandId cid);

typedef TupleTableSlot *(*ExecColumnStoreScanNext_function) (ColumnStoreHandler *handler,
							TupleTableSlot *slot);

typedef void (*ExecColumnStoreEndScan_function) (ColumnStoreHandler *handler);

typedef void (*ExecColumnStoreRescan_function) (ColumnStoreHandler *handler);

typedef void (*ExecColumnStoreMarkPos_function) (ColumnStoreHandler *handler);

typedef void (*ExecColumnStoreRestrPos_function) (ColumnStoreHandler *handler);

typedef void (*ExecColumnStoreClose_function) (ColumnStoreHandler *handler);

typedef void (*ExecColumnStoreTruncate_function) (Relation rel);

typedef int (*ExecColumnStoreSample_function) (Relation onerel, int elevel,
												HeapTuple *rows, int targrows,
												double *totalrows,
												double *totaldeadrows);
/*
 * ColumnStoreRoutine is the struct returned by a column store's handler
 * function.  It provides pointers to the callback functions needed by the
 * planner and executor.
 *
 * More function pointers are likely to be added in the future. Therefore
 * it's recommended that the handler initialize the struct with
 * makeNode(ColumnStoreRoutine) so that all fields are set to NULL. This will
 * ensure that no fields are accidentally left undefined.
 */
typedef struct ColumnStoreRoutine
{
	NodeTag		type;

	/* open a column store, return opaque pointer */
	ExecColumnStoreOpen_function ExecColumnStoreOpen;

	/* insert a single row into the column store */
	ExecColumnStoreInsert_function	ExecColumnStoreInsert;

	/* insert a batch of rows into the column store */
	ExecColumnStoreBatchInsert_function	ExecColumnStoreBatchInsert;

	/* return next tuple in scan or NULL when there's no more */
	ExecColumnStoreScanNext_function ExecColumnStoreScanNext;

	/* end column store scan and free any resources used for scan */
	ExecColumnStoreEndScan_function ExecColumnStoreEndScan;

	/* reset scan position back to the first tuple */
	ExecColumnStoreRescan_function ExecColumnStoreRescan;

	/* mark the current position of a column store scan */
	ExecColumnStoreMarkPos_function ExecColumnStoreMarkPos;

	/* restore scan position to the previously marked position */
	ExecColumnStoreRestrPos_function ExecColumnStoreRestrPos;

	/* close a column store */
	ExecColumnStoreClose_function ExecColumnStoreClose;

	/* truncate column store */
	ExecColumnStoreTruncate_function ExecColumnStoreTruncate;

	/* Populated an array of sample rows */
	ExecColumnStoreSample_function ExecColumnStoreSample;
} ColumnStoreRoutine;


/* prototypes for functions in catalog/colstore.c */
extern ColumnStoreHandler *BuildColumnStoreHandler(Relation cstore,
						bool for_write, LOCKMODE lockmode, Snapshot snapshot);
extern void CloseColumnStore(ColumnStoreHandler *handler);

extern Oid GetColumnStoreHandlerByRelId(Oid relid);
extern ColumnStoreRoutine *GetColumnStoreRoutine(Oid csthandler);
extern ColumnStoreRoutine *GetColumnStoreRoutineByRelId(Oid relid);
extern ColumnStoreRoutine *GetColumnStoreRoutineForRelation(Relation relation,
															bool makecopy);

#endif   /* COLSTOREAPI_H */
