/*-------------------------------------------------------------------------
 *
 * nodeColumnStorescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeColumnStorescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODECOLSTORESCAN_H
#define NODECOLSTORESCAN_H

#include "nodes/execnodes.h"

extern ColumnStoreScanState *ExecInitColumnStoreScan(ColumnStoreScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecColumnStoreScan(ColumnStoreScanState *node);
extern void ExecEndColumnStoreScan(ColumnStoreScanState *node);
extern void ExecColumnStoreScanMarkPos(ColumnStoreScanState *node);
extern void ExecColumnStoreScanRestrPos(ColumnStoreScanState *node);
extern void ExecReScanColumnStoreScan(ColumnStoreScanState *node);

#endif   /* NODECOLSTORESCAN_H */
