/*-------------------------------------------------------------------------
 *
 * seqlocal.c
 *	  Local sequence access manager
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/access/sequence/seqlocal.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/seqamapi.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/rel.h"
#include "utils/typcache.h"

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS	32

static bytea *seqam_local_reloptions(Datum reloptions, bool validate);
static Datum seqam_local_init(Relation seqrel, Form_pg_sequence seq,
							  int64 restart_value, bool restart_requested,
							  bool is_init);
static int64 seqam_local_alloc(Relation seqrel, SequenceHandle *seqh,
							   int64 nrequested, int64 *last);
static void seqam_local_setval(Relation seqrel, SequenceHandle *seqh,
							   int64 new_value);
static Datum seqam_local_get_state(Relation seqrel, SequenceHandle *seqh);
static void seqam_local_set_state(Relation seqrel, SequenceHandle *seqh,
								  Datum amstate);

static char *
seqam_local_state_get_part(StringInfo buf, char *ptr, bool *found)
{
	*found = false;

	resetStringInfo(buf);

	while (*ptr != ',' && *ptr != ')')
	{
		char		ch = *ptr++;

		if (ch == '\0')
		{
			*found = false;
			return ptr;
		}

		appendStringInfoChar(buf, ch);

		if (!*found)
			*found = true;
	}

	return ptr;
}

Datum
seqam_local_state_in(PG_FUNCTION_ARGS)
{
	char	   *state_str = PG_GETARG_CSTRING(0);
	char	   *ptr;
	Datum		d;
	bool		found;
	StringInfoData		buf;
	LocalSequenceState *state = palloc(sizeof(LocalSequenceState));

	ptr = state_str;
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '(')
		goto malformed;
	ptr++;

	initStringInfo(&buf);

	/* last_value is required */
	ptr = seqam_local_state_get_part(&buf, ptr, &found);
	if (!found)
		goto malformed;
	d = DirectFunctionCall1(int8in, CStringGetDatum(buf.data));
	state->last_value = DatumGetInt64(d);
	if (*ptr == ',')
		ptr++;

	/* is_called is optional */
	ptr = seqam_local_state_get_part(&buf, ptr, &found);
	if (found)
	{
		d = DirectFunctionCall1(boolin, CStringGetDatum(buf.data));
		state->is_called = DatumGetBool(d);
		if (*ptr == ',')
			ptr++;
	}
	else
		state->is_called = true;

	/* log_cnt is optional */
	ptr = seqam_local_state_get_part(&buf, ptr, &found);
	if (found)
	{
		d = DirectFunctionCall1(int4in, CStringGetDatum(buf.data));
		state->log_cnt = DatumGetInt32(d);
		if (*ptr == ',')
			ptr++;
	}
	else
		state->log_cnt = 0;

	if (*ptr != ')' || *(++ptr) != '\0')
		goto malformed;

	PG_RETURN_POINTER(state);

malformed:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("malformed seqam_local_state literal: \"%s\"",
					state_str)));
}

Datum
seqam_local_state_out(PG_FUNCTION_ARGS)
{
	LocalSequenceState *state = (LocalSequenceState *) PG_GETARG_POINTER(0);
	StringInfoData		buf;

	initStringInfo(&buf);

	appendStringInfo(&buf, "("UINT64_FORMAT",%s,%d)",
					 state->last_value, state->is_called ? "t" : "f",
					 state->log_cnt);

	PG_RETURN_CSTRING(buf.data);
}

/*
 * Handler function for the local sequence access method.
 */
Datum
seqam_local_handler(PG_FUNCTION_ARGS)
{
	SeqAmRoutine *seqam = makeNode(SeqAmRoutine);

	seqam->StateTypeOid = SEQAMLOCALSTATEOID;
	seqam->amoptions = seqam_local_reloptions;
	seqam->Init = seqam_local_init;
	seqam->Alloc = seqam_local_alloc;
	seqam->Setval = seqam_local_setval;
	seqam->GetState = seqam_local_get_state;
	seqam->SetState = seqam_local_set_state;

	PG_RETURN_POINTER(seqam);
}

/*
 * seqam_local_reloptions()
 *
 * Parse and verify the reloptions of a local sequence.
 */
static bytea *
seqam_local_reloptions(Datum reloptions, bool validate)
{
	if (validate && PointerIsValid(DatumGetPointer(reloptions)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("local sequence does not accept any storage parameters")));

	return NULL;
}

/*
 * seqam_local_init()
 *
 * Initialize local sequence
 */
static Datum
seqam_local_init(Relation seqrel, Form_pg_sequence seq, int64 restart_value,
				 bool restart_requested, bool is_init)
{
	LocalSequenceState	   *localseq;

	if (is_init)
		localseq = palloc0(sizeof(LocalSequenceState));
	else
		localseq = (LocalSequenceState *) seq->amstate;

	/*
	 * If this is a new sequence or RESTART was provided in ALTER we should
	 * reset our state to that new starting point.
	 */
	if (is_init || restart_requested)
	{
		localseq->last_value = restart_value;
		localseq->is_called = false;
	}

	localseq->log_cnt = 0;

	return PointerGetDatum(localseq);
}

/*
 * seqam_local_alloc()
 *
 * Allocate a new range of values for a local sequence.
 */
static int64
seqam_local_alloc(Relation seqrel, SequenceHandle *seqh, int64 nrequested,
				  int64 *last)
{
	int64		incby,
				maxv,
				minv,
				log,
				fetch,
				result,
				next,
				rescnt = 0;
	bool		is_cycled,
				is_called,
				logit = false;
	Form_pg_sequence	seq;
	LocalSequenceState *localseq;

	seq = sequence_read_options(seqh);
	localseq = (LocalSequenceState *) seq->amstate;

	next = result = localseq->last_value;
	incby = seq->increment_by;
	maxv = seq->max_value;
	minv = seq->min_value;
	is_cycled = seq->is_cycled;
	fetch = nrequested;
	log = localseq->log_cnt;
	is_called = localseq->is_called;

	/* We are returning last_value if not is_called so fetch one less value. */
	if (!is_called)
	{
		nrequested--;
		fetch--;
	}

	/*
	 * Decide whether we should emit a WAL log record.  If so, force up the
	 * fetch count to grab SEQ_LOG_VALS more values than we actually need to
	 * cache.  (These will then be usable without logging.)
	 *
	 * If this is the first nextval after a checkpoint, we must force a new
	 * WAL record to be written anyway, else replay starting from the
	 * checkpoint would fail to advance the sequence past the logged values.
	 * In this case we may as well fetch extra values.
	 */
	if (log < fetch || !is_called)
	{
		/* Forced log to satisfy local demand for values. */
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}
	else if (sequence_needs_wal(seqh))
	{
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}

	/* Fetch new result value if is_called. */
	if (is_called)
	{
		rescnt += sequence_increment(seqrel, &next, 1, minv, maxv, incby,
									 is_cycled, true);
		result = next;
	}

	/* Fetch as many values as was requested by backend. */
	if (rescnt < nrequested)
		rescnt += sequence_increment(seqrel, &next, nrequested - rescnt, minv,
									 maxv, incby, is_cycled, false);

	/* Last value available for calling backend. */
	*last = next;
	/* Values we made available to calling backend can't be counted as cached. */
	log -= rescnt;

	/* We might need to fetch even more values for our own caching. */
	if (rescnt < fetch)
		rescnt += sequence_increment(seqrel, &next, fetch - rescnt, minv,
									 maxv, incby, is_cycled, false);

	fetch -= rescnt;
	log -= fetch;				/* adjust for any unfetched numbers */
	Assert(log >= 0);

	/*
	 * Log our cached data.
	 */
	localseq->last_value = *last;
	localseq->log_cnt = log;
	localseq->is_called = true;

	sequence_start_update(seqh, logit);

	if (logit)
	{
		localseq->last_value = next;
		localseq->log_cnt = 0;
		localseq->is_called = true;

		sequence_save_state(seqh, PointerGetDatum(localseq), true);
	}

	localseq->last_value = *last;
	localseq->log_cnt = log;
	localseq->is_called = true;

	sequence_save_state(seqh, PointerGetDatum(localseq), false);

	sequence_finish_update(seqh);

	return result;
}

/*
 * seqam_local_setval()
 *
 * Set value of a local sequence
 */
static void
seqam_local_setval(Relation seqrel, SequenceHandle *seqh, int64 new_value)
{
	Datum				amstate;
	LocalSequenceState *localseq;

	amstate = sequence_read_state(seqh);
	localseq = (LocalSequenceState *) DatumGetPointer(amstate);

	localseq->last_value = new_value;		/* last fetched number */
	localseq->is_called = true;
	localseq->log_cnt = 0;			/* how much is logged */

	sequence_start_update(seqh, true);
	sequence_save_state(seqh, PointerGetDatum(localseq), true);
	sequence_finish_update(seqh);

	sequence_release_tuple(seqh);
}

/*
 * seqam_local_get_state()
 *
 * Dump state of a local sequence (for pg_dump)
 */
static Datum
seqam_local_get_state(Relation seqrel, SequenceHandle *seqh)
{
	Datum				amstate;
	LocalSequenceState *localseq = palloc(sizeof(LocalSequenceState));

	amstate = sequence_read_state(seqh);
	memcpy(localseq, DatumGetPointer(amstate), sizeof(LocalSequenceState));
	sequence_release_tuple(seqh);

	return PointerGetDatum(localseq);
}

/*
 * seqam_local_set_state()
 *
 * Restore previously dumped state of local sequence (used by pg_dump)
*/
static void
seqam_local_set_state(Relation seqrel, SequenceHandle *seqh,
					  Datum amstate)
{
	Form_pg_sequence	seq;
	LocalSequenceState *localseq;

	seq = sequence_read_options(seqh);
	localseq = (LocalSequenceState *) DatumGetPointer(amstate);

	sequence_check_range(localseq->last_value, seq->min_value, seq->max_value,
						 "last_value");

	sequence_start_update(seqh, true);
	sequence_save_state(seqh, amstate, true);
	sequence_finish_update(seqh);

	sequence_release_tuple(seqh);
}
