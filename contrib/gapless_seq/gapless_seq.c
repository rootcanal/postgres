/*-------------------------------------------------------------------------
 *
 * gapless_seq.c
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/gapless_seq/gapless_seq.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/seqamapi.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

/*------------------------------------------------------------
 *
 * Sequence Access Manager / Gapless sequence implementation
 *
 *------------------------------------------------------------
 */

typedef struct GaplessSequenceState
{
	int64   last_value;
	uint32	xid;
	bool	is_called;
} GaplessSequenceState;

typedef struct GaplessValue
{
	Oid		seqid;
	int64	last_value;
	bool	is_called;
} GaplessValue;

#define GAPLESS_SEQ_NAMESPACE "gapless_seq"
#define VALUES_TABLE_NAME "seqam_gapless_values"
#define VALUES_TABLE_COLUMNS 3

static bytea *seqam_gapless_reloptions(Datum reloptions, bool validate);
static Datum seqam_gapless_init(Relation seqrel, Form_pg_sequence seq,
								int64 restart_value, bool restart_requested,
								bool is_init);
static int64 seqam_gapless_alloc(Relation seqrel, SequenceHandle *seqh,
								 int64 nrequested, int64 *last);
static void seqam_gapless_setval(Relation seqrel, SequenceHandle *seqh,
								 int64 new_value);
static Datum seqam_gapless_get_state(Relation seqrel, SequenceHandle *seqh);
static void seqam_gapless_set_state(Relation seqrel, SequenceHandle *seqh,
									Datum amstate);

PG_FUNCTION_INFO_V1(seqam_gapless_handler);
PG_FUNCTION_INFO_V1(seqam_gapless_state_in);
PG_FUNCTION_INFO_V1(seqam_gapless_state_out);

static FormData_pg_sequence *wait_for_sequence(SequenceHandle *seqh,
											   TransactionId local_xid);
static Relation open_values_rel(void);
static HeapTuple get_last_value_tup(Relation rel, Oid seqid);
static void set_last_value_tup(Relation rel, Oid seqid, HeapTuple oldtuple,
							   int64 last_value, bool is_called);

Datum
seqam_gapless_state_in(PG_FUNCTION_ARGS)
{
	char	   *state_str = PG_GETARG_CSTRING(0);
	char	   *ptr, *end;
	Datum		d;
	GaplessSequenceState *state = palloc(sizeof(LocalSequenceState));

	ptr = state_str;
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '(')
		goto malformed;
	ptr++;

	end = ptr;
	while (*end != ',' && *end != ')')
	{
		char		ch = *end++;

		if (ch == '\0')
			goto malformed;
	}

	d = DirectFunctionCall1(int8in,
							CStringGetDatum(pnstrdup(ptr, end-ptr)));
	state->last_value = DatumGetInt64(d);

	/* is_called is optional */
	if (*end == ',')
	{
		ptr = ++end;

		end = state_str+strlen(state_str)-1;

		if (*end != ')')
			goto malformed;

		d = DirectFunctionCall1(boolin,
								CStringGetDatum(pnstrdup(ptr, end-ptr)));
		state->is_called = DatumGetBool(d);
	}
	else
		state->is_called = true;

	state->xid = InvalidTransactionId;

	PG_RETURN_POINTER(state);

malformed:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("malformed seqam_gapless_state literal: \"%s\"",
					state_str)));
}

Datum
seqam_gapless_state_out(PG_FUNCTION_ARGS)
{
	GaplessSequenceState   *state;
	StringInfoData			buf;

	state = (GaplessSequenceState *) PG_GETARG_POINTER(0);

	initStringInfo(&buf);

	appendStringInfo(&buf, "("UINT64_FORMAT",%s)",
					 state->last_value, state->is_called ? "t" : "f");

	PG_RETURN_CSTRING(buf.data);
}

/*
 * Handler function for the gapless sequence access method.
 */
Datum
seqam_gapless_handler(PG_FUNCTION_ARGS)
{
	SeqAmRoutine   *seqam = makeNode(SeqAmRoutine);
	Oid				nspid,
					typid;

	nspid = get_namespace_oid("gapless_seq", false);
	typid = GetSysCacheOid2(TYPENAMENSP,
							PointerGetDatum("seqam_gapless_state"),
							ObjectIdGetDatum(nspid));

	if (!OidIsValid(typid))
		elog(ERROR, "cache lookup failed for type \"seqam_gapless_state\"");

	seqam->StateTypeOid = typid;
	seqam->amoptions = seqam_gapless_reloptions;
	seqam->Init = seqam_gapless_init;
	seqam->Alloc = seqam_gapless_alloc;
	seqam->Setval = seqam_gapless_setval;
	seqam->GetState = seqam_gapless_get_state;
	seqam->SetState = seqam_gapless_set_state;

	PG_RETURN_POINTER(seqam);
}

/*
 * seqam_gapless_reloptions()
 *
 * Parse and verify the reloptions of a gapless sequence.
 */
static bytea *
seqam_gapless_reloptions(Datum reloptions, bool validate)
{
	if (validate && PointerIsValid(DatumGetPointer(reloptions)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gapless sequence does not accept any storage parameters")));

	return NULL;
}

/*
 * seqam_gapless_init()
 *
 * Initialize gapless sequence
 *
 */
static Datum
seqam_gapless_init(Relation seqrel, Form_pg_sequence seq, int64 restart_value,
				 bool restart_requested, bool is_init)
{
	Oid			seqrelid = seqrel->rd_id;
	Relation	valrel;
	HeapTuple	valtuple;
	GaplessSequenceState   *seqstate;

	if (is_init)
		seqstate = palloc0(sizeof(GaplessSequenceState));
	else
		seqstate = (GaplessSequenceState *) seq->amstate;

	/*
	 * If this is a new sequence or RESTART was provided in ALTER we should
	 * reset our state to that new starting point.
	 */
	if (is_init || restart_requested)
	{
		seqstate->last_value = restart_value;
		seqstate->is_called = false;
	}

	seqstate->xid = GetTopTransactionId();

	/* Load current value if this is existing sequence. */
	valrel = open_values_rel();
	valtuple = get_last_value_tup(valrel, seqrelid);

	/*
	 * If this is new sequence or restart was provided or if there is
	 * no previous stored value for the sequence we should store it in
	 * the values table.
	 */
	if (is_init || restart_requested || !HeapTupleIsValid(valtuple))
		set_last_value_tup(valrel, seqrelid, valtuple, seqstate->last_value,
						   seqstate->is_called);

	/* Now we are done with values relation, but keep the lock. */
	heap_close(valrel, NoLock);

	return PointerGetDatum(seqstate);
}

/*
 * seqam_gapless_alloc()
 *
 * Allocate new value for gapless sequence.
 */
static int64
seqam_gapless_alloc(Relation seqrel, SequenceHandle *seqh, int64 nrequested,
					int64 *last)
{
	int64		result;
	Oid			seqrelid = RelationGetRelid(seqrel);
	Relation	valrel;
	HeapTuple	tuple;
	Form_pg_sequence		seq;
	GaplessSequenceState   *seqstate;
	TransactionId local_xid = GetTopTransactionId();

	/* Wait until the sequence is locked by us. */
	seq = wait_for_sequence(seqh, local_xid);
	seqstate = (GaplessSequenceState *) seq->amstate;

	/* Read the last value from our transactional table (if any). */
	valrel = open_values_rel();
	tuple = get_last_value_tup(valrel, seqrelid);

	/* Last value found get next value. */
	if (HeapTupleIsValid(tuple))
	{
		GaplessValue *v = (GaplessValue *) GETSTRUCT(tuple);
		result = v->last_value;

		if (v->is_called)
			(void) sequence_increment(seqrel, &result, 1, seq->min_value,
									  seq->max_value,
									  seq->increment_by,
									  seq->is_cycled, true);
	}
	else /* No last value, start from beginning. */
		result = seq->start_value;

	/* Insert or update the last value tuple. */
	set_last_value_tup(valrel, seqrelid, tuple, result, true);

	/* Now we are done with values relation, but keep the lock. */
	heap_close(valrel, NoLock);

	/* We always WAL log for gapless sequence. */
	sequence_start_update(seqh, true);
	seqstate->last_value = result;
	seqstate->is_called = true;
	if (seqstate->xid != local_xid)
		seqstate->xid = local_xid;
	sequence_save_state(seqh, PointerGetDatum(seqstate), true);
	sequence_finish_update(seqh);

	*last = result;

	return result;
}

/*
 * seqam_gapless_setval()
 *
 * Setval support (we don't allow setval on gapless)
 */
static void
seqam_gapless_setval(Relation seqrel, SequenceHandle *seqh, int64 new_value)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("setval() is not supported for gapless sequences")));
}

/*
 * seqam_gapless_get_state()
 *
 * Dump state of a gapless sequence (for pg_dump)
 */
static Datum
seqam_gapless_get_state(Relation seqrel, SequenceHandle *seqh)
{
	Oid				seqrelid = RelationGetRelid(seqrel);
	Relation		valrel;
	HeapTuple		tuple;
	GaplessSequenceState   *seqstate = palloc0(sizeof(GaplessSequenceState));

	/*
	 * Get the last value from the values table, if not found use the values
	 * from sequence typle.
	 */
	valrel = open_values_rel();
	tuple = get_last_value_tup(valrel, seqrelid);
	heap_close(valrel, RowExclusiveLock);

	if (HeapTupleIsValid(tuple))
	{
		GaplessValue *v = (GaplessValue *) GETSTRUCT(tuple);
		seqstate->last_value = v->last_value;
		seqstate->is_called = v->is_called;
	}
	else
	{
		Datum		amstate = sequence_read_state(seqh);

		memcpy(seqstate, DatumGetPointer(amstate),
			   sizeof(GaplessSequenceState));
		sequence_release_tuple(seqh);
	}

	return PointerGetDatum(seqstate);
}

/*
 * seqam_gapless_set_state()
 *
 * Restore previously dumpred state of gapless sequence
 */
static void
seqam_gapless_set_state(Relation seqrel, SequenceHandle *seqh,
						Datum amstate)
{
	Oid				seqrelid = RelationGetRelid(seqrel);
	Relation		valrel;
	HeapTuple		tuple;
	FormData_pg_sequence   *seq;
	GaplessSequenceState   *seqstate;
	TransactionId local_xid = GetTopTransactionId();

	/* Wait until the sequence is locked by us. */
	seq = wait_for_sequence(seqh, local_xid);
	seqstate = (GaplessSequenceState *) DatumGetPointer(amstate);

	sequence_check_range(seqstate->last_value, seq->min_value, seq->max_value,
						 "last_value");

	/* Read the last value from our transactional table (if any). */
	valrel = open_values_rel();
	tuple = get_last_value_tup(valrel, seqrelid);

	/* Insert or update the last value tuple. */
	set_last_value_tup(valrel, seqrelid, tuple, seqstate->last_value,
					   seqstate->is_called);

	/* Now we are done with values relation, but keep the lock. */
	heap_close(valrel, NoLock);

	/* Save to updated sequence. */
	sequence_start_update(seqh, true);
	sequence_save_state(seqh, amstate, true);
	sequence_finish_update(seqh);

	sequence_release_tuple(seqh);
}

/*
 * Lock the sequence for current transaction.
 */
static FormData_pg_sequence *
wait_for_sequence(SequenceHandle *seqh, TransactionId local_xid)
{
	FormData_pg_sequence   *seq = sequence_read_options(seqh);
	GaplessSequenceState   *seqstate;

	seqstate = (GaplessSequenceState *) seq->amstate;

	/*
	 * Read and lock the sequence for our transaction, there can't be any
	 * concurrent transactions accessing the sequence at the same time.
	 */
	while (seqstate->xid != local_xid &&
		   TransactionIdIsInProgress(seqstate->xid))
	{
		/*
		 * Release tuple to avoid dead locks and wait for the concurrent tx
		 * to finish.
		 */
		sequence_release_tuple(seqh);
		XactLockTableWait(seqstate->xid, NULL, NULL, XLTW_None);
		/* Reread the sequence. */
		seq = sequence_read_options(seqh);
	}

	return seq;
}

/*
 * Open the relation used for storing last value in RowExclusive lock mode.
 */
static Relation
open_values_rel(void)
{
	RangeVar   *rv;
	Oid			valrelid;
	Relation	valrel;

	rv = makeRangeVar(GAPLESS_SEQ_NAMESPACE, VALUES_TABLE_NAME, -1);
	valrelid = RangeVarGetRelid(rv, RowExclusiveLock, false);
	valrel = heap_open(valrelid, RowExclusiveLock);

	return valrel;
}

/*
 * Read the last value tuple from the values table.
 *
 * Can return NULL if tuple is not found.
 */
static HeapTuple
get_last_value_tup(Relation rel, Oid seqid)
{
	ScanKey		key;
	SysScanDesc	scan;
	HeapTuple	tuple;

	key = (ScanKey) palloc(sizeof(ScanKeyData) * 1);

	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqid)
		);

	/* FIXME: should use index */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);
	systable_endscan(scan);

	return tuple;
}

/*
 * Insert or update the last value tuple.
 *
 * The write access to the table must be serialized by the wait_for_sequence
 * so that we don't have to have any retry scheme here.
*/
static void
set_last_value_tup(Relation rel, Oid seqid, HeapTuple oldtuple,
				   int64 last_value, bool is_called)
{
	bool		nulls[VALUES_TABLE_COLUMNS];
	Datum		values[VALUES_TABLE_COLUMNS];
	TupleDesc	tupDesc = RelationGetDescr(rel);
	HeapTuple	tuple;

	if (!HeapTupleIsValid(oldtuple))
	{
		memset(nulls, false, VALUES_TABLE_COLUMNS * sizeof(bool));
		values[0] = ObjectIdGetDatum(seqid);
		values[1] = Int64GetDatum(last_value);
		values[2] = BoolGetDatum(is_called);

		tuple = heap_form_tuple(tupDesc, values, nulls);
		simple_heap_insert(rel, tuple);
	}
	else
	{
		bool replaces[VALUES_TABLE_COLUMNS];

		replaces[0] = false;
		replaces[1] = true;
		replaces[2] = true;

		nulls[1] = false;
		nulls[2] = false;
		values[1] = Int64GetDatum(last_value);
		values[2] = BoolGetDatum(is_called);

		tuple = heap_modify_tuple(oldtuple, tupDesc, values, nulls, replaces);
		simple_heap_update(rel, &tuple->t_self, tuple);
	}

	CatalogUpdateIndexes(rel, tuple);
}
