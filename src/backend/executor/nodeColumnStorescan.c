/*-------------------------------------------------------------------------
 *
 * nodeColumnStorescan.c
 *	  Routines to handle column store scan nodes.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeColumnStorescan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecColumnStoreScan		- sequentially scans a column store.
 *		ExecInitColumnStoreScan	- creates and initializes the scan node.
 *		ExecEndColumnStoreScan	- releases any storage allocated.
 *
 */
#include "postgres.h"

#include "colstore/colstoreapi.h"
#include "executor/executor.h"
#include "executor/nodeColumnStorescan.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "miscadmin.h"

/* ----------------------------------------------------------------
 *		ExecColumnStoreScan
 *			and static support routines
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ColumnStoreNext(ColumnStoreScanState *node)
{
	TupleTableSlot *slot;
	ExecColumnStoreScanNext_function cstscan;

	cstscan = node->csthandler->csh_ColumnStoreRoutine->ExecColumnStoreScanNext;
	if (cstscan == NULL)
		elog(ERROR, "Scan routine not provided by column store AM");

	slot = node->ss.ss_ScanTupleSlot;
	slot = cstscan(node->csthandler, slot);

	return slot;
}

static bool
ColumnStoreRecheck(ColumnStoreScanState *node)
{
	/* are there specific conditions to recheck? */
	return true;
}

TupleTableSlot *
ExecColumnStoreScan(ColumnStoreScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) ColumnStoreNext,
					(ExecScanRecheckMtd) ColumnStoreRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitColumnStoreScan
 * ----------------------------------------------------------------
 */
ColumnStoreScanState *
ExecInitColumnStoreScan(ColumnStoreScan *node, EState *estate, int eflags)
{
	ColumnStoreScanState	   *colscanstate;
	Relation	currentRelation;
	Index		scanrelid = node->scan.scanrelid;

	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	colscanstate = makeNode(ColumnStoreScanState);
	colscanstate->ss.ps.plan = (Plan *) node;
	colscanstate->ss.ps.state = estate;

	/* don't set flags because the suport routines are not implemented yet */
#if 0
	colscanstate->eflags = (eflags & (EXEC_FLAG_REWIND |
									  EXEC_FLAG_BACKWARD |
									  EXEC_FLAG_MARK));
#endif

	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
	colscanstate->ss.ss_currentRelation = currentRelation;
	colscanstate->csthandler =
		BuildColumnStoreHandler(currentRelation, false,
								AccessShareLock, estate->es_snapshot);

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &colscanstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	colscanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist, &colscanstate->ss.ps);
	colscanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual, &colscanstate->ss.ps);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &colscanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &colscanstate->ss);

	/*
	 * initialize scan relation
	 */
	ExecAssignScanType(&colscanstate->ss, RelationGetDescr(currentRelation));
	colscanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&colscanstate->ss.ps);
	ExecAssignScanProjectionInfoWithVarno(&colscanstate->ss, scanrelid);

	return colscanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndColumnStoreScan
 * ----------------------------------------------------------------
 */
void
ExecEndColumnStoreScan(ColumnStoreScanState *node)
{
	Relation	relation;
	ExecColumnStoreEndScan_function cstendscan;

	cstendscan = node->csthandler->csh_ColumnStoreRoutine->ExecColumnStoreEndScan;
	if (cstendscan == NULL)
		elog(ERROR, "EndScan routine not provided by column store AM");

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;

	/* Call the ExecColumnStoreEndScan API function */
	cstendscan(node->csthandler);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecColumnStoreScanMarkPos
 * ----------------------------------------------------------------
 */
void
ExecColumnStoreScanMarkPos(ColumnStoreScanState *node)
{
	ExecColumnStoreMarkPos_function markpos;

	Assert(node->eflags & EXEC_FLAG_MARK);

	markpos = node->csthandler->csh_ColumnStoreRoutine->ExecColumnStoreMarkPos;

	if (!markpos)
		elog(ERROR, "MarkPos routine not supplied by column store AM");

	markpos(node->csthandler);
}

/* ----------------------------------------------------------------
 *		ExecColumnStoreScanRestrPos
 * ----------------------------------------------------------------
 */
void
ExecColumnStoreScanRestrPos(ColumnStoreScanState *node)
{
	ExecColumnStoreRestrPos_function restore;

	Assert(node->eflags & EXEC_FLAG_MARK);

	restore = node->csthandler->csh_ColumnStoreRoutine->ExecColumnStoreRestrPos;

	if (!restore)
		elog(ERROR, "RestrPos routine not supplied by column store AM");

	restore(node->csthandler);
}

/* ----------------------------------------------------------------
 *		ExecReScanColumnStoreScan
 *
 *		Rescans the col store relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanColumnStoreScan(ColumnStoreScanState *node)
{
	ColumnStoreHandler *handler = node->csthandler;
	ExecColumnStoreRescan_function rescan;

	rescan = handler->csh_ColumnStoreRoutine->ExecColumnStoreRescan;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (rescan == NULL)
		elog(ERROR, "ReScan routine not supplied by column store AM");

	rescan(node->csthandler);

	ExecScanReScan(&node->ss);
}
