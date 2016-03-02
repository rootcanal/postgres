/*-------------------------------------------------------------------------
 *
 * seqamapi.c
 *	  general sequence access method routines
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/sequence/seqamapi.c
 *
 *
 * Sequence access method allows the SQL Standard Sequence objects to be
 * managed according to either the default access method or a pluggable
 * replacement. Each sequence can only use one access method at a time,
 * though different sequence access methods can be in use by different
 * sequences at the same time.
 *
 * The SQL Standard assumes that each Sequence object is completely controlled
 * from the current database node, preventing any form of clustering mechanisms
 * from controlling behaviour. Sequence access methods are general purpose
 * though designed specifically to address the needs of Sequences working as
 * part of a multi-node "cluster", though that is not defined here, nor are
 * there dependencies on anything outside of this module, nor any particular
 * form of clustering.
 *
 * The SQL Standard behaviour, also the historical PostgreSQL behaviour, is
 * referred to as the "Local" SeqAM. That is also the basic default.  Local
 * SeqAM assumes that allocations from the sequence will be contiguous, so if
 * user1 requests a range of values and is given 500-599 as values for their
 * backend then the next user to make a request will be given a range starting
 * with 600.
 *
 * The SeqAM mechanism allows us to override the Local behaviour, for use with
 * clustering systems. When multiple masters can request ranges of values it
 * would break the assumption of contiguous allocation. It seems likely that
 * the SeqAM would also wish to control node-level caches for sequences to
 * ensure good performance. The CACHE option and other options may be
 * overridden by the _init API call, if needed, though in general having
 * cacheing per backend and per node seems desirable.
 *
 * SeqAM allows calls to allocate a new range of values, reset the sequence to
 * a new value and to define options for the AM module. The on-disk format of
 * Sequences is the same for all AMs, except that each sequence has a SeqAm
 * defined private-data column, amstate.
 *
 * SeqAMs work similarly to IndexAMs in many ways. pg_class.relam stores the
 * Oid of the SeqAM, just as we do for IndexAm. The relcache stores AM
 * information in much the same way for indexes and sequences, and management
 * of options is similar also.
 *
 * Note that the SeqAM API calls are synchronous. It is up to the SeqAM to
 * decide how that is handled, for example, whether there is a higher level
 * cache at instance level to amortise network traffic in cluster.
 *
 * The SeqAM is identified by Oid of corresponding tuple in pg_am.
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/seqamapi.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * GetSeqAmRoutine - call the specified access method handler routine
 * to get its SeqAmRoutine struct.
 */
SeqAmRoutine *
GetSeqAmRoutine(Oid amhandler)
{
	Datum			datum;
	SeqAmRoutine   *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (SeqAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, SeqAmRoutine))
		elog(ERROR, "sequence access method handler function %u did not return an SeqAmRoutine struct",
			 amhandler);

	return routine;
}

/*
 * GetSeqAmRoutineByAMId - look up the handler of the sequence access method
 * for the given OID, and retrieve its SeqAmRoutine struct.
 */
SeqAmRoutine *
GetSeqAmRoutineByAMId(Oid amoid)
{
	HeapTuple		tuple;
	regproc			amhandler;
	Form_pg_am		amform;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u",
			 amoid);
	amform = (Form_pg_am) GETSTRUCT(tuple);
	amhandler = amform->amhandler;

	/* Complain if handler OID is invalid */
	if (!RegProcedureIsValid(amhandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("sequence access method \"%s\" does not have a handler",
						NameStr(amform->amname))));
	ReleaseSysCache(tuple);

	/* And finally, call the handler function. */
	return GetSeqAmRoutine(amhandler);
}

/*
 * GetSeqAmRoutineForRelation - look up the handler of the sequence access
 * method for the given sequence Relation.
 */
SeqAmRoutine *
GetSeqAmRoutineForRelation(Relation relation)
{
	SeqAmRoutine *seqam;
	SeqAmRoutine *cseqam;

	if (relation->rd_seqamroutine == NULL)
	{
		if (!OidIsValid(relation->rd_rel->relam))
		{
			char *relname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)),
													   RelationGetRelationName(relation));
			elog(ERROR, "relation %s isn't sequence", relname);
		}

		seqam = GetSeqAmRoutineByAMId(relation->rd_rel->relam);
		/* Save the data for later reuse in CacheMemoryContext */
		cseqam = (SeqAmRoutine *) MemoryContextAlloc(CacheMemoryContext,
												   sizeof(SeqAmRoutine));
		memcpy(cseqam, seqam, sizeof(SeqAmRoutine));
		relation->rd_seqamroutine = cseqam;
	}

	return relation->rd_seqamroutine;
}

/*
 * ValidateSeqAmRoutine - to sanity check for the SeqAmRoutine returned by a
 * handler.
 *
 * At the moment we only check that the StateTypeOid is valid type oid and that
 * the type it points to is fixed width and will fit into the sequence page.
 */
void
ValidateSeqAmRoutine(SeqAmRoutine *routine)
{
#define MAX_SEQAM_STATE_LEN \
	MaxHeapTupleSize - MinHeapTupleSize - MAXALIGN(offsetof(FormData_pg_sequence, amstate))

	Oid	stattypeoid = routine->StateTypeOid;

	if (!get_typisdefined(stattypeoid))
		ereport(ERROR,
				(errmsg("type with OID %u does not exist",
						stattypeoid)));

	if (TypeIsToastable(stattypeoid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sequence access method state data type \"%s\" must not be toastable",
						format_type_be(stattypeoid))));

	if (get_typlen(stattypeoid) >= MAX_SEQAM_STATE_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sequence access method state data type \"%s\" exceeds maximum allowed length (%lu)",
						format_type_be(stattypeoid), MAX_SEQAM_STATE_LEN)));

}
