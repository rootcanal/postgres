/*-------------------------------------------------------------------------
 *
 * seqamapi.h
 *	  Public header file for Sequence access method.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/seqamapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQAMAPI_H
#define SEQAMAPI_H

#include "fmgr.h"

#include "access/htup.h"
#include "commands/sequence.h"
#include "utils/relcache.h"
#include "storage/buf.h"
#include "storage/bufpage.h"

struct SequenceHandle;
typedef struct SequenceHandle SequenceHandle;

typedef bytea * (*ParseRelOptions_function) (Datum reloptions,
											 bool validate);

typedef Datum (*SeqAMInit_function) (Relation seqrel,
									 Form_pg_sequence seq,
									 int64 restart_value,
									 bool restart_requested,
									 bool is_init);

typedef int64 (*SeqAMAlloc_function) (Relation seqrel,
									  SequenceHandle *seqh,
									  int64 nrequested,
									  int64 *last);

typedef void (*SeqAMSetval_function) (Relation seqrel,
									  SequenceHandle *seqh,
									  int64 new_value);

typedef Datum (*SeqAMGetState_function) (Relation seqrel,
										 SequenceHandle *seqh);
typedef void (*SeqAMSetState_function) (Relation seqrel, SequenceHandle *seqh,
										Datum amstate);

typedef struct SeqAmRoutine
{
	NodeTag		type;

	/* Custom columns needed by the AM */
	Oid			StateTypeOid;

	/* Function for parsing reloptions */
	ParseRelOptions_function	amoptions;

	/* Initialization */
	SeqAMInit_function			Init;

	/* nextval handler */
	SeqAMAlloc_function			Alloc;

	/* State manipulation functions */
	SeqAMSetval_function		Setval;
	SeqAMGetState_function		GetState;
	SeqAMSetState_function		SetState;
} SeqAmRoutine;

extern SeqAmRoutine *GetSeqAmRoutine(Oid seqamhandler);
extern SeqAmRoutine *GetSeqAmRoutineByAMId(Oid amoid);
extern SeqAmRoutine *GetSeqAmRoutineForRelation(Relation relation);
extern void ValidateSeqAmRoutine(SeqAmRoutine *routine);

extern void sequence_open(Oid seqrelid, SequenceHandle *seqh);
extern void sequence_close(SequenceHandle *seqh);
extern Form_pg_sequence sequence_read_options(SequenceHandle *seqh);
extern Datum sequence_read_state(SequenceHandle *seqh);
extern void sequence_start_update(SequenceHandle *seqh, bool dowal);
extern void sequence_save_state(SequenceHandle *seqh, Datum seqstate,
								bool dowal);
extern void sequence_finish_update(SequenceHandle *seqh);
extern void sequence_release_tuple(SequenceHandle *seqh);
extern bool sequence_needs_wal(SequenceHandle *seqh);

extern int64 sequence_increment(Relation seqrel, int64 *value, int64 incnum,
								int64 minv, int64 maxv, int64 incby,
								bool is_cycled, bool report_errors);
extern void sequence_check_range(int64 value, int64 min_value,
								 int64 max_value, const char *valname);
extern int64 sequence_get_restart_value(List *options, int64 default_value,
										bool *found);

/* We need this public for serval3 */
typedef struct LocalSequenceState
{
	int64   last_value;
	int32	log_cnt;
	bool	is_called;
} LocalSequenceState;

extern Datum seqam_local_state_in(PG_FUNCTION_ARGS);
extern Datum seqam_local_state_out(PG_FUNCTION_ARGS);
extern Datum seqam_local_handler(PG_FUNCTION_ARGS);

#endif   /* SEQAMAPI_H */
