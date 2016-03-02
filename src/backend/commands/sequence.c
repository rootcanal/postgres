/*-------------------------------------------------------------------------
 *
 * sequence.c
 *	  PostgreSQL sequences support code.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/sequence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "access/seqamapi.h"
#include "access/transam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/syscache.h"


/*
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC	  0x1717

typedef struct sequence_magic
{
	uint32		magic;
} sequence_magic;

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 */
typedef struct SeqTableData
{
	Oid			relid;			/* pg_class OID of this sequence (hash key) */
	Oid			filenode;		/* last seen relfilenode of this sequence */
	LocalTransactionId lxid;	/* xact in which we last did a seq op */
	bool		last_valid;		/* do we have a valid "last" value? */
	int64		last;			/* value last returned by nextval */
	int64		cached;			/* last value already cached for nextval */
	/* if last != cached, we have not used up all the cached values */
	int64		increment;		/* copy of sequence's increment field */
	/* note that increment is zero until we first do read_seq_tuple() */
} SeqTableData;

typedef SeqTableData *SeqTable;

static HTAB *seqhashtab = NULL; /* hash table for SeqTable items */

struct SequenceHandle
{
	SeqTable	elm;
	Relation	rel;
	Buffer		buf;
	Oid			statetyp;
	int16		statetyplen;
	bool		statetypbyval;
	HeapTupleData tup;
	bool		inupdate;
};

/*
 * last_used_seq is updated by nextval() to point to the last used
 * sequence.
 */
static SeqTableData *last_used_seq = NULL;

static HeapTuple build_seq_tuple(Relation rel, SeqAmRoutine *seqam,
								 Form_pg_sequence new, int64 restart_value);
static void fill_seq_with_data(Relation rel, HeapTuple tuple);
static int64 nextval_internal(Oid relid);
static Relation open_share_lock(SeqTable seq);
static void create_seq_hashtable(void);
static void init_params(List *options, bool isInit,
			Form_pg_sequence new, List **owned_by);
static void process_owned_by(Relation seqrel, List *owned_by);
static void log_sequence_tuple(Relation seqrel, HeapTuple tuple,
							   Buffer buf, Page page);
static HeapTuple sequence_read_tuple(SequenceHandle *seqh);

/*
 * Build template column definition for a sequence relation.
*/
static ColumnDef *
makeSeqColumnDef(void)
{
	ColumnDef  *coldef = makeNode(ColumnDef);

	coldef->inhcount = 0;
	coldef->is_local = true;
	coldef->is_not_null = true;
	coldef->is_from_type = false;
	/* Force plain storage. */
	coldef->storage = 'p';
	coldef->raw_default = NULL;
	coldef->cooked_default = NULL;
	coldef->collClause = NULL;
	coldef->collOid = InvalidOid;
	coldef->constraints = NIL;
	coldef->location = -1;

	return coldef;
}

/*
 * Add additional sequence AM columns to the sequence column definition list.
 */
static List *
BuildSeqColumnDefList(Oid amstateTypeOid)
{
	List	   *seqcols;
	int			colid;

	seqcols = NIL;
	for (colid = SEQ_COL_FIRSTCOL; colid <= SEQ_COL_LASTCOL; colid++)
	{
		ColumnDef  *coldef = makeSeqColumnDef();

		switch (colid)
		{
			case SEQ_COL_STARTVAL:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "start_value";
				break;
			case SEQ_COL_INCBY:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "increment_by";
				break;
			case SEQ_COL_MAXVALUE:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "max_value";
				break;
			case SEQ_COL_MINVALUE:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "min_value";
				break;
			case SEQ_COL_CACHE:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "cache_value";
				break;
			case SEQ_COL_CYCLE:
				coldef->typeName = makeTypeNameFromOid(BOOLOID, -1);
				coldef->colname = "is_cycled";
				break;
			case SEQ_COL_AMSTATE:
				coldef->typeName = makeTypeNameFromOid(amstateTypeOid, -1);
				coldef->colname = "amstate";
				break;
		}

		seqcols = lappend(seqcols, coldef);
	}

	return seqcols;
}

/*
 * DefineSequence
 *				Creates a new sequence relation
 */
ObjectAddress
DefineSequence(CreateSeqStmt *seq)
{
	FormData_pg_sequence new;
	List	   *owned_by;
	CreateStmt *stmt = makeNode(CreateStmt);
	Oid			seqoid;
	Oid			seqamid;
	ObjectAddress address;
	Relation	rel;
	HeapTuple	tuple;
	List	   *seqcols;
	SeqAmRoutine *seqam;

	/* Unlogged sequences are not implemented -- not clear if useful. */
	if (seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unlogged sequences are not supported")));

	/*
	 * If if_not_exists was given and a relation with the same name already
	 * exists, bail out. (Note: we needn't check this when not if_not_exists,
	 * because DefineRelation will complain anyway.)
	 */
	if (seq->if_not_exists)
	{
		RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock, &seqoid);
		if (OidIsValid(seqoid))
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							seq->sequence->relname)));
			return InvalidObjectAddress;
		}
	}

	/* Check and set all option values */
	init_params(seq->options, true, &new, &owned_by);

	if (seq->accessMethod)
		seqamid = get_am_oid(seq->accessMethod, AMTYPE_SEQUENCE, false);
	else
		seqamid = LOCAL_SEQAM_OID;

	seqam = GetSeqAmRoutineByAMId(seqamid);
	/* Build column definitions. */
	seqcols = BuildSeqColumnDefList(seqam->StateTypeOid);

	stmt->relation = seq->sequence;
	stmt->inhRelations = NIL;
	stmt->constraints = NIL;
	stmt->options = seq->amoptions;
	stmt->oncommit = ONCOMMIT_NOOP;
	stmt->tablespacename = NULL;
	stmt->if_not_exists = seq->if_not_exists;
	stmt->tableElts = seqcols;

	address = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId, seqamid,
							 NULL);
	seqoid = address.objectId;
	Assert(seqoid != InvalidOid);

	rel = heap_open(seqoid, AccessExclusiveLock);

	/* Build new sequence tuple and store it. */
	tuple = build_seq_tuple(rel, seqam, &new, new.start_value);
	fill_seq_with_data(rel, tuple);

	/* process OWNED BY if given */
	if (owned_by)
		process_owned_by(rel, owned_by);

	heap_close(rel, NoLock);

	return address;
}


/*
 * Reset a sequence to its initial value.
 *
 * The change is made transactionally, so that on failure of the current
 * transaction, the sequence will be restored to its previous state.
 * We do that by creating a whole new relfilenode for the sequence; so this
 * works much like the rewriting forms of ALTER TABLE.
 *
 * Caller is assumed to have acquired AccessExclusiveLock on the sequence,
 * which must not be released until end of transaction.  Caller is also
 * responsible for permissions checking.
 */
void
ResetSequence(Oid seqrelid)
{
	HeapTuple	tuple;
	Relation	seqrel;
	SequenceHandle		seqh;
	Form_pg_sequence	seq;
	TupleDesc			tupDesc;
	Datum	   values[SEQ_COL_LASTCOL];
	bool	   nulls[SEQ_COL_LASTCOL];
	SeqAmRoutine *seqam;

	/*
	 * Read and lock the old page.
	 */
	sequence_open(seqrelid, &seqh);
	tuple = sequence_read_tuple(&seqh);
	seqrel = seqh.rel;
	seqam = GetSeqAmRoutineForRelation(seqrel);

	/*
	 * Copy the existing sequence tuple.
	 */
	tuple = heap_copytuple(tuple);

	/* Now we're done with the old page */
	sequence_release_tuple(&seqh);

	seq = (Form_pg_sequence) GETSTRUCT(tuple);
	tupDesc = RelationGetDescr(seqrel);
	heap_deform_tuple(tuple, tupDesc, values, nulls);
	values[SEQ_COL_AMSTATE - 1] = seqam->Init(seqrel, seq, seq->start_value,
											  true, false);
	tuple = heap_form_tuple(tupDesc, values, nulls);

	/*
	 * Create a new storage file for the sequence.  We want to keep the
	 * sequence's relfrozenxid at 0, since it won't contain any unfrozen XIDs.
	 * Same with relminmxid, since a sequence will never contain multixacts.
	 */
	RelationSetNewRelfilenode(seqrel, seqh.rel->rd_rel->relpersistence,
							  InvalidTransactionId, InvalidMultiXactId);

	/*
	 * Insert the modified tuple into the new storage file.
	 */
	fill_seq_with_data(seqrel, tuple);

	/* Clear local cache so that we don't think we have cached numbers */
	/* Note that we do not change the currval() state */
	seqh.elm->cached = seqh.elm->last;

	/* And we're done, close the sequence. */
	sequence_close(&seqh);
}

/*
 * Build sequence tuple based on the sequence form and fill in the
 * sequence AM specific info as well.
 */
static HeapTuple
build_seq_tuple(Relation rel, SeqAmRoutine *seqam, Form_pg_sequence new,
				int64 restart_value)
{
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	Datum		values[SEQ_COL_LASTCOL];
	bool		nulls[SEQ_COL_LASTCOL];

	tupDesc = RelationGetDescr(rel);

	memset(nulls, 0, sizeof(nulls));

	values[SEQ_COL_STARTVAL - 1] = Int64GetDatumFast(new->start_value);
	values[SEQ_COL_INCBY - 1] = Int64GetDatumFast(new->increment_by);
	values[SEQ_COL_MAXVALUE - 1] = Int64GetDatumFast(new->max_value);
	values[SEQ_COL_MINVALUE - 1] = Int64GetDatumFast(new->min_value);
	values[SEQ_COL_CACHE - 1] = Int64GetDatumFast(new->cache_value);
	values[SEQ_COL_CYCLE - 1] = BoolGetDatum(new->is_cycled);
	values[SEQ_COL_AMSTATE - 1] = seqam->Init(rel, new, restart_value,
											  false, true);
	tuple = heap_form_tuple(tupDesc, values, nulls);

	return tuple;
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 */
static void
fill_seq_with_data(Relation rel, HeapTuple tuple)
{
	Buffer		buf;
	Page		page;
	sequence_magic *sm;
	OffsetNumber offnum;

	/* Initialize first page of relation with special magic number */

	buf = ReadBuffer(rel, P_NEW);
	Assert(BufferGetBlockNumber(buf) == 0);

	page = BufferGetPage(buf);

	PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(page);
	sm->magic = SEQ_MAGIC;

	/* Now insert sequence tuple */

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Since VACUUM does not process sequences, we have to force the tuple to
	 * have xmin = FrozenTransactionId now.  Otherwise it would become
	 * invisible to SELECTs after 2G transactions.  It is okay to do this
	 * because if the current transaction aborts, no other xact will ever
	 * examine the sequence tuple anyway.
	 */
	HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
	HeapTupleHeaderSetXminFrozen(tuple->t_data);
	HeapTupleHeaderSetCmin(tuple->t_data, FirstCommandId);
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	ItemPointerSet(&tuple->t_data->t_ctid, 0, FirstOffsetNumber);

	/*
	 * If something needs to be WAL logged, make sure that xid was acquired,
	 * so this transaction's commit will trigger a WAL flush and wait for
	 * syncrep. It's sufficient to ensure the toplevel transaction has a xid,
	 * no need to assign xids subxacts, that'll already trigger a appropriate
	 * wait. (Has to be done outside of critical section).
	 */
	if (RelationNeedsWAL(rel))
		GetTopTransactionId();

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
						 InvalidOffsetNumber, false, false);
	if (offnum != FirstOffsetNumber)
		elog(ERROR, "failed to add sequence tuple to page");

	/* XLOG stuff */
	log_sequence_tuple(rel, tuple, buf, page);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * Replace the type of amstate column.
 *
 * We don't do AlterTable here as that produces dead columns which we don't
 * want. This is safe because the sequence page is controlled by code in this
 * module and isn't changed the same way as a table.
 *
 * TODO: check if anybody is depending on the row-type associated with the
 * sequence.
 */
static void
replace_sequence_amstate_col(Oid seqrelid, Oid typid)
{
	Relation	attr_rel;
	Datum		values[Natts_pg_attribute];
	bool		nulls[Natts_pg_attribute];
	bool		replace[Natts_pg_attribute];
	HeapTuple	tp,
				attr_tuple,
				newattr_tuple;
	Form_pg_type	typtup;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for type %u", typid);

	typtup = (Form_pg_type) GETSTRUCT(tp);

	memset(nulls, 0, sizeof(nulls));
	memset(replace, 0, sizeof(replace));

	replace[Anum_pg_attribute_atttypid - 1] = true;
	replace[Anum_pg_attribute_attlen - 1] = true;
	replace[Anum_pg_attribute_attbyval - 1] = true;
	replace[Anum_pg_attribute_attalign - 1] = true;

	values[Anum_pg_attribute_atttypid - 1] = ObjectIdGetDatum(typid);
	values[Anum_pg_attribute_attlen - 1] = Int16GetDatum(typtup->typlen);
	values[Anum_pg_attribute_attbyval - 1] = BoolGetDatum(typtup->typbyval);
	values[Anum_pg_attribute_attalign - 1] = CharGetDatum(typtup->typalign);

	/* Build DROP command for amstate of old AM. */
	attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

	attr_tuple = SearchSysCache2(ATTNUM,
								 ObjectIdGetDatum(seqrelid),
								 Int16GetDatum(SEQ_COL_AMSTATE));
	if (!HeapTupleIsValid(attr_tuple))		/* shouldn't happen */
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 SEQ_COL_AMSTATE, seqrelid);

	newattr_tuple = heap_modify_tuple(attr_tuple, RelationGetDescr(attr_rel),
									  values, nulls, replace);
	simple_heap_update(attr_rel, &newattr_tuple->t_self, newattr_tuple);
	CatalogUpdateIndexes(attr_rel, newattr_tuple);

	ReleaseSysCache(tp);
	heap_freetuple(newattr_tuple);
	ReleaseSysCache(attr_tuple);

	heap_close(attr_rel, RowExclusiveLock);
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 */
ObjectAddress
AlterSequence(AlterSeqStmt *stmt)
{
	Oid			seqrelid;
	Oid			oldamid;
	Oid			seqamid;
	HeapTuple	tuple;
	Relation	seqrel;
	Form_pg_sequence seq,
					 new;
	List	   *owned_by;
	ObjectAddress address;
	int64		restart_value;
	bool		restart_requested;
	SequenceHandle seqh;
	SeqAmRoutine *oldseqam;

	/* Open and lock sequence. */
	seqrelid = RangeVarGetRelid(stmt->sequence, AccessExclusiveLock, stmt->missing_ok);

	if (seqrelid == InvalidOid)
	{
		ereport(NOTICE,
				(errmsg("relation \"%s\" does not exist, skipping",
						stmt->sequence->relname)));
		return InvalidObjectAddress;
	}

	sequence_open(seqrelid, &seqh);
	seqrel = seqh.rel;
	oldamid = seqrel->rd_rel->relam;
	oldseqam = GetSeqAmRoutineByAMId(oldamid);

	/* allow ALTER to sequence owner only */
	if (!pg_class_ownercheck(seqrelid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   stmt->sequence->relname);

	/* lock page' buffer and read tuple into new sequence structure */
	tuple = sequence_read_tuple(&seqh);
	seq = (Form_pg_sequence) GETSTRUCT(tuple);

	/* Copy old values of options into workspace */
	tuple = heap_copytuple(tuple);
	new = (Form_pg_sequence) GETSTRUCT(tuple);

	/* Check and set new values */
	init_params(stmt->options, false, new, &owned_by);

	if (stmt->accessMethod)
		seqamid = get_am_oid(stmt->accessMethod, AMTYPE_SEQUENCE, false);
	else
		seqamid = oldamid;

	restart_value = sequence_get_restart_value(stmt->options, new->start_value,
											   &restart_requested);

	/*
	 * If we are changing sequence AM, we need to alter the sequence relation.
	 */
	if (seqamid != oldamid)
	{
		ObjectAddress	myself,
						referenced;
		Relation        pgcrel;
		HeapTuple       pgctup,
						newpgctuple;
		HeapTuple       seqamtup;
		Form_pg_am		form_am;
		Datum			reloptions;
		Datum			values[Natts_pg_class];
		bool			nulls[Natts_pg_class];
		bool			replace[Natts_pg_class];
		static char	   *validnsps[2];
		SeqAmRoutine	   *newseqam;

		oldseqam = GetSeqAmRoutineByAMId(oldamid);

		/*
		 * If RESTART [WITH] option was not specified in ALTER SEQUENCE
		 * statement, we use nextval of the old sequence AM to provide
		 * restart point for the new sequence AM.
		 */
		if (!restart_requested)
		{
			int64 last;
			restart_value = oldseqam->Alloc(seqrel, &seqh, 1, &last);
		}

		sequence_check_range(restart_value, new->min_value, new->max_value, "RESTART");

		/* We don't need the old sequence tuple anymore. */
		sequence_release_tuple(&seqh);

		/* Parse the new reloptions. */
		seqamtup = SearchSysCache1(AMOID, ObjectIdGetDatum(seqamid));
		if (!HeapTupleIsValid(seqamtup))
			elog(ERROR, "cache lookup failed for sequence access method %u",
				 seqamid);

		newseqam = GetSeqAmRoutineByAMId(seqamid);

		form_am = (Form_pg_am) GETSTRUCT(seqamtup);

		validnsps[0] = NameStr(form_am->amname);
		validnsps[1] = NULL;

		reloptions = transformRelOptions((Datum) 0, stmt->amoptions, NULL,
										 validnsps, true, false);

		(void) am_reloptions(newseqam->amoptions, reloptions, true);
		ReleaseSysCache(seqamtup);

		/* Update the pg_class entry. */
		pgcrel = heap_open(RelationRelationId, RowExclusiveLock);
		pgctup = SearchSysCache1(RELOID, ObjectIdGetDatum(seqrelid));
		if (!HeapTupleIsValid(pgctup))
			elog(ERROR, "pg_class entry for sequence %u unavailable",
				 seqrelid);

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replace, false, sizeof(replace));

		values[Anum_pg_class_relam - 1] = ObjectIdGetDatum(seqamid);
		replace[Anum_pg_class_relam - 1] = true;

		if (reloptions != (Datum) 0)
			values[Anum_pg_class_reloptions - 1] = reloptions;
		else
			nulls[Anum_pg_class_reloptions - 1] = true;
		replace[Anum_pg_class_reloptions - 1] = true;

		newpgctuple = heap_modify_tuple(pgctup, RelationGetDescr(pgcrel),
										values, nulls, replace);

		simple_heap_update(pgcrel, &newpgctuple->t_self, newpgctuple);

		CatalogUpdateIndexes(pgcrel, newpgctuple);

		heap_freetuple(newpgctuple);
		ReleaseSysCache(pgctup);

		heap_close(pgcrel, NoLock);

		CommandCounterIncrement();

		/*
		 * Create a new storage file for the sequence.
		 * And change the type definition.
		 *
		 * We can't use AlterTable internals here because the sequence
		 * has to have the expected number of columns and no
		 * attisdropped = true columns.
		 */
		RelationSetNewRelfilenode(seqrel, seqrel->rd_rel->relpersistence,
								  InvalidTransactionId, InvalidMultiXactId);
		replace_sequence_amstate_col(seqrelid, newseqam->StateTypeOid);
		CommandCounterIncrement();

		/* Rebuild the sequence tuple and save it. */
		tuple = build_seq_tuple(seqrel, newseqam, new, restart_value);
		fill_seq_with_data(seqh.rel, tuple);

		/* Remove dependency on previous SeqAM */
		deleteDependencyRecordsForClass(RelationRelationId, seqrelid,
										AccessMethodRelationId,
										DEPENDENCY_NORMAL);

		/* Record dependency on new SeqAM */
		myself.classId = RelationRelationId;
		myself.objectId = seqrelid;
		myself.objectSubId = 0;
		referenced.classId = AccessMethodRelationId;
		referenced.objectId = seqamid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
	else
	{
		Datum	   newamstate;

		sequence_check_range(restart_value, new->min_value, new->max_value,
							 restart_requested ? "RESTART" : "START");

		/* Let the new sequence AM initialize. */
		newamstate = oldseqam->Init(seqrel, new, restart_value,
									restart_requested, false);

		sequence_start_update(&seqh, true);
		memcpy(seq, new, offsetof(FormData_pg_sequence, amstate));
		sequence_save_state(&seqh, newamstate, true);
		sequence_finish_update(&seqh);
		sequence_release_tuple(&seqh);
	}

	/* Clear local cache so that we don't think we have cached numbers */
	/* Note that we do not change the currval() state */
	seqh.elm->cached = seqh.elm->last;

	/* process OWNED BY if given */
	if (owned_by)
		process_owned_by(seqrel, owned_by);

	InvokeObjectPostAlterHook(RelationRelationId, seqrelid, 0);

	ObjectAddressSet(address, RelationRelationId, seqrelid);

	sequence_close(&seqh);

	return address;
}


/*
 * Note: nextval with a text argument is no longer exported as a pg_proc
 * entry, but we keep it around to ease porting of C code that may have
 * called the function directly.
 */
Datum
nextval(PG_FUNCTION_ARGS)
{
	text	   *seqin = PG_GETARG_TEXT_P(0);
	RangeVar   *sequence;
	Oid			relid;

	sequence = makeRangeVarFromNameList(textToQualifiedNameList(seqin));

	/*
	 * XXX: This is not safe in the presence of concurrent DDL, but acquiring
	 * a lock here is more expensive than letting nextval_internal do it,
	 * since the latter maintains a cache that keeps us from hitting the lock
	 * manager more than once per transaction.  It's not clear whether the
	 * performance penalty is material in practice, but for now, we do it this
	 * way.
	 */
	relid = RangeVarGetRelid(sequence, NoLock, false);

	PG_RETURN_INT64(nextval_internal(relid));
}

Datum
nextval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	PG_RETURN_INT64(nextval_internal(relid));
}

/*
 * Sequence AM independent part of nextval() that does permission checking,
 * returns cached values and then calls out to the SeqAM specific nextval part.
 */
static int64
nextval_internal(Oid relid)
{
	SeqTable	elm;
	Relation	seqrel;
	Form_pg_sequence seq_form;
	int64		last,
				result;
	SequenceHandle seqh;
	SeqAmRoutine *seqam;

	/* open and AccessShareLock sequence */
	sequence_open(relid, &seqh);
	elm = seqh.elm;
	seqrel = seqh.rel;
	seqam = GetSeqAmRoutineForRelation(seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("nextval()");

	/*
	 * Forbid this during parallel operation because, to make it work, the
	 * cooperating backends would need to share the backend-local cached
	 * sequence information.  Currently, we don't support that.
	 */
	PreventCommandIfParallelMode("nextval()");

	if (elm->last != elm->cached)		/* some numbers were cached */
	{
		Assert(elm->last_valid);
		Assert(elm->increment != 0);
		elm->last += elm->increment;
		sequence_close(&seqh);
		last_used_seq = elm;
		return elm->last;
	}

	/* lock page' buffer and read tuple */
	seq_form = (Form_pg_sequence) GETSTRUCT(sequence_read_tuple(&seqh));

	result = seqam->Alloc(seqrel, &seqh, seq_form->cache_value, &last);

	/* save info in local cache */
	elm->last = result;			/* last returned number */
	elm->cached = last;			/* last fetched number */
	elm->last_valid = true;

	last_used_seq = elm;

	sequence_release_tuple(&seqh);
	sequence_close(&seqh);

	return result;
}

Datum
currval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	SequenceHandle seqh;

	/* open and AccessShareLock sequence */
	sequence_open(relid, &seqh);

	if (pg_class_aclcheck(seqh.elm->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqh.rel))));

	if (!seqh.elm->last_valid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("currval of sequence \"%s\" is not yet defined in this session",
						RelationGetRelationName(seqh.rel))));

	result = seqh.elm->last;

	sequence_close(&seqh);

	PG_RETURN_INT64(result);
}

Datum
lastval(PG_FUNCTION_ARGS)
{
	Relation	seqrel;
	int64		result;

	if (last_used_seq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lastval is not yet defined in this session")));

	/* Someone may have dropped the sequence since the last nextval() */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(last_used_seq->relid)))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lastval is not yet defined in this session")));

	seqrel = open_share_lock(last_used_seq);

	/* nextval() must have already been called for this sequence */
	Assert(last_used_seq->last_valid);

	if (pg_class_aclcheck(last_used_seq->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	result = last_used_seq->last;
	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

/*
 * Implement the setval procedure.
 */
Datum
setval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		next = PG_GETARG_INT64(1);
	SeqTable	elm;
	Relation	seqrel;
	SequenceHandle seqh;
	SeqAmRoutine *seqam;

	/* open and AccessShareLock sequence */
	sequence_open(relid, &seqh);
	elm = seqh.elm;
	seqrel = seqh.rel;
	seqam = GetSeqAmRoutineForRelation(seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("setval()");

	/*
	 * Forbid this during parallel operation because, to make it work, the
	 * cooperating backends would need to share the backend-local cached
	 * sequence information.  Currently, we don't support that.
	 */
	PreventCommandIfParallelMode("setval()");

	seqam->Setval(seqrel, &seqh, next);

	/* Reset local cached data */
	elm->last = next;		/* last returned number */
	elm->last_valid = true;
	elm->cached = elm->last;

	last_used_seq = elm;

	sequence_close(&seqh);

	PG_RETURN_INT64(next);
}

/*
 * Implement the 3 arg setval procedure.
 *
 * This is a cludge for supporting old dumps.
 *
 * Check that the target sequence is local one and then convert this call
 * to the seqam_restore call with apropriate data.
 */
Datum
setval3_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		next = PG_GETARG_INT64(1);
	bool		iscalled = PG_GETARG_BOOL(2);
	LocalSequenceState	state;
	SeqTable	elm;
	Relation	seqrel;
	SequenceHandle seqh;
	SeqAmRoutine *seqam;

	/* open and AccessShareLock sequence */
	sequence_open(relid, &seqh);
	elm = seqh.elm;
	seqrel = seqh.rel;
	seqam = GetSeqAmRoutineForRelation(seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
							RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("setval()");

	/* Make sure the target sequence is 'local' sequence. */
	if (seqrel->rd_rel->relam != LOCAL_SEQAM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("the setval(oid, bigint, bool) function can only be called for \"local\" sequences")));

	/* Build the state and pass it to sequence AM. */
	state.last_value = next;
	state.log_cnt = 0;
	state.is_called = iscalled;
	seqam->SetState(seqh.rel, &seqh, PointerGetDatum(&state));

	/* Set the currval() state only if iscalled = true */
	if (iscalled)
	{
		elm->last = next;		/* last returned number */
		elm->last_valid = true;
	}

	/* Reset local cached data */
	elm->cached = elm->last;

	last_used_seq = elm;

	sequence_close(&seqh);

	PG_RETURN_INT64(next);
}

/*
 * Open the sequence and acquire AccessShareLock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire AccessShareLock.  We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
static Relation
open_share_lock(SeqTable seq)
{
	LocalTransactionId thislxid = MyProc->lxid;

	/* Get the lock if not already held in this xact */
	if (seq->lxid != thislxid)
	{
		ResourceOwner currentOwner;

		currentOwner = CurrentResourceOwner;
		PG_TRY();
		{
			CurrentResourceOwner = TopTransactionResourceOwner;
			LockRelationOid(seq->relid, AccessShareLock);
		}
		PG_CATCH();
		{
			/* Ensure CurrentResourceOwner is restored on error */
			CurrentResourceOwner = currentOwner;
			PG_RE_THROW();
		}
		PG_END_TRY();
		CurrentResourceOwner = currentOwner;

		/* Flag that we have a lock in the current xact */
		seq->lxid = thislxid;
	}

	/* We now know we have AccessShareLock, and can safely open the rel */
	return relation_open(seq->relid, NoLock);
}

/*
 * Creates the hash table for storing sequence data
 */
static void
create_seq_hashtable(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SeqTableData);

	seqhashtab = hash_create("Sequence values", 16, &ctl,
							 HASH_ELEM | HASH_BLOBS);
}

/*
 * Given a relation OID, open and share-lock the sequence.
 */
void
sequence_open(Oid relid, SequenceHandle *seqh)
{
	SeqTable	elm;
	Relation	seqrel;
	bool		found;

	/* Find or create a hash table entry for this sequence */
	if (seqhashtab == NULL)
		create_seq_hashtable();

	elm = (SeqTable) hash_search(seqhashtab, &relid, HASH_ENTER, &found);

	/*
	 * Initialize the new hash table entry if it did not exist already.
	 *
	 * NOTE: seqtable entries are stored for the life of a backend (unless
	 * explicitly discarded with DISCARD). If the sequence itself is deleted
	 * then the entry becomes wasted memory, but it's small enough that this
	 * should not matter.
	 */
	if (!found)
	{
		/* relid already filled in */
		elm->filenode = InvalidOid;
		elm->lxid = InvalidLocalTransactionId;
		elm->last_valid = false;
		elm->last = elm->cached = elm->increment = 0;
	}

	/*
	 * Open the sequence relation.
	 */
	seqrel = open_share_lock(elm);

	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence",
						RelationGetRelationName(seqrel))));

	/*
	 * If the sequence has been transactionally replaced since we last saw it,
	 * discard any cached-but-unissued values.  We do not touch the currval()
	 * state, however.
	 */
	if (seqrel->rd_rel->relfilenode != elm->filenode)
	{
		elm->filenode = seqrel->rd_rel->relfilenode;
		elm->cached = elm->last;
	}

	/* Return results */
	seqh->elm = elm;
	seqh->rel = seqrel;
	seqh->buf = InvalidBuffer;
	seqh->tup.t_data = NULL;
	seqh->tup.t_len = 0;
	seqh->statetyp = 6025; /* TODO */
	seqh->statetyplen = -1;
	seqh->statetypbyval = false;
	seqh->inupdate = false;
}

/*
 * Given the sequence handle, unlock the page buffer and close the relation
 */
void
sequence_close(SequenceHandle *seqh)
{
	Assert(!seqh->inupdate);

	relation_close(seqh->rel, NoLock);
}

/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 */
static HeapTuple
sequence_read_tuple(SequenceHandle *seqh)
{
	Page		page;
	Buffer		buf;
	ItemId		lp;
	sequence_magic *sm;

	if (seqh->tup.t_data != NULL)
		return &seqh->tup;

	seqh->buf = buf = ReadBuffer(seqh->rel, 0);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(buf);
	sm = (sequence_magic *) PageGetSpecialPointer(page);

	if (sm->magic != SEQ_MAGIC)
		elog(ERROR, "bad magic number in sequence \"%s\": %08X",
			 RelationGetRelationName(seqh->rel), sm->magic);

	lp = PageGetItemId(page, FirstOffsetNumber);
	Assert(ItemIdIsNormal(lp));

	/* Note we currently only bother to set these two fields of the tuple */
	seqh->tup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	seqh->tup.t_len = ItemIdGetLength(lp);

	/*
	 * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
	 * a sequence, which would leave a non-frozen XID in the sequence tuple's
	 * xmax, which eventually leads to clog access failures or worse. If we
	 * see this has happened, clean up after it.  We treat this like a hint
	 * bit update, ie, don't bother to WAL-log it, since we can certainly do
	 * this again if the update gets lost.
	 */
	Assert(!(seqh->tup.t_data->t_infomask & HEAP_XMAX_IS_MULTI));
	if (HeapTupleHeaderGetRawXmax(seqh->tup.t_data) != InvalidTransactionId)
	{
		HeapTupleHeaderSetXmax(seqh->tup.t_data, InvalidTransactionId);
		seqh->tup.t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
		seqh->tup.t_data->t_infomask |= HEAP_XMAX_INVALID;
		MarkBufferDirtyHint(buf, true);
	}

	/* update our copy of the increment if needed */
	if (seqh->elm->increment == 0)
	{
		Form_pg_sequence seq = (Form_pg_sequence) GETSTRUCT(&seqh->tup);
		seqh->elm->increment = seq->increment_by;
	}

	return &seqh->tup;
}

Form_pg_sequence
sequence_read_options(SequenceHandle *seqh)
{
	return (Form_pg_sequence) GETSTRUCT(sequence_read_tuple(seqh));
}

Datum
sequence_read_state(SequenceHandle *seqh)
{
	HeapTuple			tup = sequence_read_tuple(seqh);
	Form_pg_sequence	seq = (Form_pg_sequence) GETSTRUCT(tup);

	return PointerGetDatum(seq->amstate);
}

/*
 * Write a sequence tuple.
 *
 * If 'do_wal' is false, the update doesn't need to be WAL-logged. After
 * a crash, you might get an old copy of the tuple.
 *
 * We split this into 3 step process so that the tuple may be safely updated
 * inline.
 */
void
sequence_start_update(SequenceHandle *seqh, bool dowal)
{
	Assert(seqh->tup.t_data != NULL && !seqh->inupdate);


	if (seqh->statetyplen < 0)
	{
		get_typlenbyval(seqh->statetyp, &seqh->statetyplen,
						&seqh->statetypbyval);
		Assert(seqh->statetyplen > 0);
	}

	if (dowal)
		GetTopTransactionId();

	seqh->inupdate = true;

	START_CRIT_SECTION();
}

void
sequence_save_state(SequenceHandle *seqh, Datum amstate, bool dowal)
{
	HeapTuple			tup = sequence_read_tuple(seqh);
	Form_pg_sequence	seq = (Form_pg_sequence) GETSTRUCT(tup);
	Page				page;

	/*
	 * Update the state data inline.
	 *
	 * This is only needed when the provided amstate datum points to different
	 * data than what is already in the tuple.
	 */
	if (DatumGetPointer(amstate) != seq->amstate)
	{
		if (seqh->statetypbyval)
			store_att_byval(seq->amstate, amstate, seqh->statetyplen);
		else
			memmove(seq->amstate, DatumGetPointer(amstate), seqh->statetyplen);
	}

	page = BufferGetPage(seqh->buf);
	MarkBufferDirtyHint(seqh->buf, true);

	if (dowal && RelationNeedsWAL(seqh->rel))
		log_sequence_tuple(seqh->rel, &seqh->tup, seqh->buf, page);
}

void
sequence_finish_update(SequenceHandle *seqh)
{
	Assert(seqh->inupdate);

	END_CRIT_SECTION();

	seqh->inupdate = false;
}


/*
 * Release a tuple, read with sequence_read_tuple, without saving it
 */
void
sequence_release_tuple(SequenceHandle *seqh)
{
	/* Remove the tuple from cache */
	if (seqh->tup.t_data != NULL)
	{
		seqh->tup.t_data = NULL;
		seqh->tup.t_len = 0;
	}

	/* Release the page lock */
	if (BufferIsValid(seqh->buf))
	{
		UnlockReleaseBuffer(seqh->buf);
		seqh->buf = InvalidBuffer;
	}
}

/*
 * Returns true, if the next update to the sequence tuple needs to be
 * WAL-logged because it's the first update after a checkpoint.
 *
 * The sequence AM can use this as a hint, if it wants to piggyback some extra
 * actions on WAL-logged updates.
 *
 * NB: This is just a hint. even when sequence_needs_wal() returns 'false',
 * the sequence access method might decide to WAL-log an update anyway.
 */
bool
sequence_needs_wal(SequenceHandle *seqh)
{
	Page		page;
	XLogRecPtr	redoptr;

	Assert(BufferIsValid(seqh->buf));

	if (!RelationNeedsWAL(seqh->rel))
		return false;

	page = BufferGetPage(seqh->buf);
	redoptr = GetRedoRecPtr();

	return (PageGetLSN(page) <= redoptr);
}

/*
 * init_params: process the params list of CREATE or ALTER SEQUENCE,
 * and store the values into appropriate fields of *new.  Also set
 * *owned_by to any OWNED BY param, or to NIL if there is none.
 *
 * If isInit is true, fill any unspecified params with default values;
 * otherwise, do not change existing params that aren't explicitly overridden.
 *
 * Note that only syntax check is done for RESTART [WITH] parameter, the actual
 * handling of it should be done by init function of a sequence access method.
 */
static void
init_params(List *params, bool isInit,
			Form_pg_sequence new, List **owned_by)
{
	DefElem    *start_value = NULL;
	DefElem    *restart_value = NULL;
	DefElem    *increment_by = NULL;
	DefElem    *max_value = NULL;
	DefElem    *min_value = NULL;
	DefElem    *cache_value = NULL;
	DefElem    *is_cycled = NULL;
	ListCell   *param;

	*owned_by = NIL;

	foreach(param, params)
	{
		DefElem    *defel = (DefElem *) lfirst(param);

		if (strcmp(defel->defname, "increment") == 0)
		{
			if (increment_by)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			increment_by = defel;
		}
		else if (strcmp(defel->defname, "start") == 0)
		{
			if (start_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			start_value = defel;
		}
		else if (strcmp(defel->defname, "restart") == 0)
		{
			if (restart_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			restart_value = defel;
		}
		else if (strcmp(defel->defname, "maxvalue") == 0)
		{
			if (max_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			max_value = defel;
		}
		else if (strcmp(defel->defname, "minvalue") == 0)
		{
			if (min_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			min_value = defel;
		}
		else if (strcmp(defel->defname, "cache") == 0)
		{
			if (cache_value)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cache_value = defel;
		}
		else if (strcmp(defel->defname, "cycle") == 0)
		{
			if (is_cycled)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			is_cycled = defel;
		}
		else if (strcmp(defel->defname, "owned_by") == 0)
		{
			if (*owned_by)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			*owned_by = defGetQualifiedName(defel);
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/* INCREMENT BY */
	if (increment_by != NULL)
	{
		new->increment_by = defGetInt64(increment_by);
		if (new->increment_by == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("INCREMENT must not be zero")));
	}
	else if (isInit)
		new->increment_by = 1;

	/* CYCLE */
	if (is_cycled != NULL)
	{
		new->is_cycled = intVal(is_cycled->arg);
		Assert(BoolIsValid(new->is_cycled));
	}
	else if (isInit)
		new->is_cycled = false;

	/* MAXVALUE (null arg means NO MAXVALUE) */
	if (max_value != NULL && max_value->arg)
	{
		new->max_value = defGetInt64(max_value);
	}
	else if (isInit || max_value != NULL)
	{
		if (new->increment_by > 0)
			new->max_value = SEQ_MAXVALUE;		/* ascending seq */
		else
			new->max_value = -1;	/* descending seq */
	}

	/* MINVALUE (null arg means NO MINVALUE) */
	if (min_value != NULL && min_value->arg)
	{
		new->min_value = defGetInt64(min_value);
	}
	else if (isInit || min_value != NULL)
	{
		if (new->increment_by > 0)
			new->min_value = 1; /* ascending seq */
		else
			new->min_value = SEQ_MINVALUE;		/* descending seq */
	}

	/* crosscheck min/max */
	if (new->min_value >= new->max_value)
	{
		char		bufm[100],
					bufx[100];

		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->min_value);
		snprintf(bufx, sizeof(bufx), INT64_FORMAT, new->max_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("MINVALUE (%s) must be less than MAXVALUE (%s)",
						bufm, bufx)));
	}

	/* START WITH */
	if (start_value != NULL)
		new->start_value = defGetInt64(start_value);
	else if (isInit)
	{
		if (new->increment_by > 0)
			new->start_value = new->min_value;	/* ascending seq */
		else
			new->start_value = new->max_value;	/* descending seq */
	}

	/* crosscheck START */
	if (new->start_value < new->min_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, new->start_value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->min_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("START value (%s) cannot be less than MINVALUE (%s)",
						bufs, bufm)));
	}
	if (new->start_value > new->max_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, new->start_value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, new->max_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			  errmsg("START value (%s) cannot be greater than MAXVALUE (%s)",
					 bufs, bufm)));
	}

	/* CACHE */
	if (cache_value != NULL)
	{
		new->cache_value = defGetInt64(cache_value);
		if (new->cache_value <= 0)
		{
			char		buf[100];

			snprintf(buf, sizeof(buf), INT64_FORMAT, new->cache_value);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CACHE (%s) must be greater than zero",
							buf)));
		}
	}
	else if (isInit)
		new->cache_value = 1;
}

/*
 * Process an OWNED BY option for CREATE/ALTER SEQUENCE
 *
 * Ownership permissions on the sequence are already checked,
 * but if we are establishing a new owned-by dependency, we must
 * enforce that the referenced table has the same owner and namespace
 * as the sequence.
 */
static void
process_owned_by(Relation seqrel, List *owned_by)
{
	int			nnames;
	Relation	tablerel;
	AttrNumber	attnum;

	nnames = list_length(owned_by);
	Assert(nnames > 0);
	if (nnames == 1)
	{
		/* Must be OWNED BY NONE */
		if (strcmp(strVal(linitial(owned_by)), "none") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid OWNED BY option"),
				errhint("Specify OWNED BY table.column or OWNED BY NONE.")));
		tablerel = NULL;
		attnum = 0;
	}
	else
	{
		List	   *relname;
		char	   *attrname;
		RangeVar   *rel;

		/* Separate relname and attr name */
		relname = list_truncate(list_copy(owned_by), nnames - 1);
		attrname = strVal(lfirst(list_tail(owned_by)));

		/* Open and lock rel to ensure it won't go away meanwhile */
		rel = makeRangeVarFromNameList(relname);
		tablerel = relation_openrv(rel, AccessShareLock);

		/* Must be a regular or foreign table */
		if (!(tablerel->rd_rel->relkind == RELKIND_RELATION ||
			  tablerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("referenced relation \"%s\" is not a table or foreign table",
							RelationGetRelationName(tablerel))));

		/* We insist on same owner and schema */
		if (seqrel->rd_rel->relowner != tablerel->rd_rel->relowner)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("sequence must have same owner as table it is linked to")));
		if (RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("sequence must be in same schema as table it is linked to")));

		/* Now, fetch the attribute number from the system cache */
		attnum = get_attnum(RelationGetRelid(tablerel), attrname);
		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							attrname, RelationGetRelationName(tablerel))));
	}

	/*
	 * OK, we are ready to update pg_depend.  First remove any existing AUTO
	 * dependencies for the sequence, then optionally add a new one.
	 */
	markSequenceUnowned(RelationGetRelid(seqrel));

	if (tablerel)
	{
		ObjectAddress refobject,
					depobject;

		refobject.classId = RelationRelationId;
		refobject.objectId = RelationGetRelid(tablerel);
		refobject.objectSubId = attnum;
		depobject.classId = RelationRelationId;
		depobject.objectId = RelationGetRelid(seqrel);
		depobject.objectSubId = 0;
		recordDependencyOn(&depobject, &refobject, DEPENDENCY_AUTO);
	}

	/* Done, but hold lock until commit */
	if (tablerel)
		relation_close(tablerel, NoLock);
}


/*
 * Return sequence parameters, for use by information schema
 */
Datum
pg_sequence_parameters(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		isnull[5];
	Form_pg_sequence seq;
	SequenceHandle  seqh;

	/* open and AccessShareLock sequence */
	sequence_open(relid, &seqh);

	if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqh.rel))));

	tupdesc = CreateTemplateTupleDesc(5, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "start_value",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "minimum_value",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "maximum_value",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "increment",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "cycle_option",
					   BOOLOID, -1, 0);

	BlessTupleDesc(tupdesc);

	memset(isnull, 0, sizeof(isnull));

	seq = (Form_pg_sequence) GETSTRUCT(sequence_read_tuple(&seqh));

	values[0] = Int64GetDatum(seq->start_value);
	values[1] = Int64GetDatum(seq->min_value);
	values[2] = Int64GetDatum(seq->max_value);
	values[3] = Int64GetDatum(seq->increment_by);
	values[4] = BoolGetDatum(seq->is_cycled);

	sequence_release_tuple(&seqh);
	sequence_close(&seqh);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

Datum
pg_sequence_get_state(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Datum		state;
	char	   *statestr;
	SequenceHandle seqh;
	SeqAmRoutine *seqam;
	Oid			typoutput;
	bool		typisvarlena;

	/* Load the sequence AM */
	sequence_open(relid, &seqh);
	seqam = GetSeqAmRoutineForRelation(seqh.rel);

	/* Get the type output function. */
	getTypeOutputInfo(seqam->StateTypeOid, &typoutput, &typisvarlena);

	/* Get the output and convert it to string. */
	state = seqam->GetState(seqh.rel, &seqh);
	statestr = OidOutputFunctionCall(typoutput, state);

	sequence_close(&seqh);

	PG_RETURN_TEXT_P(cstring_to_text(statestr));
}

Datum
pg_sequence_set_state(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	char	   *statestr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	SequenceHandle seqh;
	SeqAmRoutine *seqam;
	Oid			typinput,
				typioparam;
	Datum		state;

	/* Load the sequence AM */
	sequence_open(relid, &seqh);
	seqam = GetSeqAmRoutineForRelation(seqh.rel);

	/* Get the type input function. */
	getTypeInputInfo(seqam->StateTypeOid, &typinput, &typioparam);

	/* Convert the string to the state type and set it as new state. */
	state = OidInputFunctionCall(typinput, statestr, typioparam, -1);
	seqam->SetState(seqh.rel, &seqh, state);

	sequence_close(&seqh);

	PG_RETURN_VOID();
}

static void
log_sequence_tuple(Relation seqrel, HeapTuple tuple,
				   Buffer buf, Page page)
{
	xl_seq_rec	xlrec;
	XLogRecPtr	recptr;

	XLogBeginInsert();
	XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

	xlrec.node = seqrel->rd_node;

	XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
	XLogRegisterData((char *) tuple->t_data, tuple->t_len);

	recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

	PageSetLSN(page, recptr);
}

void
seq_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	Buffer		buffer;
	Page		page;
	Page		localpage;
	char	   *item;
	Size		itemsz;
	xl_seq_rec *xlrec = (xl_seq_rec *) XLogRecGetData(record);
	sequence_magic *sm;

	if (info != XLOG_SEQ_LOG)
		elog(PANIC, "seq_redo: unknown op code %u", info);

	buffer = XLogInitBufferForRedo(record, 0);
	page = (Page) BufferGetPage(buffer);

	/*
	 * We always reinit the page.  However, since this WAL record type is also
	 * used for updating sequences, it's possible that a hot-standby backend
	 * is examining the page concurrently; so we mustn't transiently trash the
	 * buffer.  The solution is to build the correct new page contents in
	 * local workspace and then memcpy into the buffer.  Then only bytes that
	 * are supposed to change will change, even transiently. We must palloc
	 * the local page for alignment reasons.
	 */
	localpage = (Page) palloc(BufferGetPageSize(buffer));

	PageInit(localpage, BufferGetPageSize(buffer), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(localpage);
	sm->magic = SEQ_MAGIC;

	item = (char *) xlrec + sizeof(xl_seq_rec);
	itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_rec);

	if (PageAddItem(localpage, (Item) item, itemsz,
					FirstOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(PANIC, "seq_redo: failed to add item to page");

	PageSetLSN(localpage, lsn);

	memcpy(page, localpage, BufferGetPageSize(buffer));
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	pfree(localpage);
}

/*
 * Flush cached sequence information.
 */
void
ResetSequenceCaches(void)
{
	if (seqhashtab)
	{
		hash_destroy(seqhashtab);
		seqhashtab = NULL;
	}

	last_used_seq = NULL;
}

/*
 * Increment sequence while correctly handling overflows and min/max.
 */
int64
sequence_increment(Relation seqrel, int64 *value, int64 incnum, int64 minv,
				   int64 maxv, int64 incby, bool is_cycled, bool report_errors)
{
	int64		next = *value;
	int64		rescnt = 0;

	while (incnum)
	{
		/*
		 * Check MAXVALUE for ascending sequences and MINVALUE for descending
		 * sequences
		 */
		if (incby > 0)
		{
			/* ascending sequence */
			if ((maxv >= 0 && next > maxv - incby) ||
				(maxv < 0 && next + incby > maxv))
			{
				/*
				 * We were asked to not report errors, return without
				 * incrementing and let the caller handle it.
				 */
				if (!report_errors)
					return rescnt;
				if (!is_cycled)
				{
					char		buf[100];

					snprintf(buf, sizeof(buf), INT64_FORMAT, maxv);
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("nextval: reached maximum value of sequence \"%s\" (%s)",
								  RelationGetRelationName(seqrel), buf)));
				}
				next = minv;
			}
			else
				next += incby;
		}
		else
		{
			/* descending sequence */
			if ((minv < 0 && next < minv - incby) ||
				(minv >= 0 && next + incby < minv))
			{
				/*
				 * We were asked to not report errors, return without incrementing
				 * and let the caller handle it.
				 */
				if (!report_errors)
					return rescnt;
				if (!is_cycled)
				{
					char		buf[100];

					snprintf(buf, sizeof(buf), INT64_FORMAT, minv);
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("nextval: reached minimum value of sequence \"%s\" (%s)",
								  RelationGetRelationName(seqrel), buf)));
				}
				next = maxv;
			}
			else
				next += incby;
		}
		rescnt++;
		incnum--;
	}

	*value = next;

	return rescnt;
}


/*
 * Check that new value, minimum and maximum are valid.
 *
 * Used by sequence AMs during sequence initialization to validate
 * the sequence parameters.
 */
void
sequence_check_range(int64 value, int64 min_value, int64 max_value, const char *valname)
{
	if (value < min_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, min_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s value (%s) cannot be less than MINVALUE (%s)",
						valname, bufs, bufm)));
	}

	if (value > max_value)
	{
		char		bufs[100],
					bufm[100];

		snprintf(bufs, sizeof(bufs), INT64_FORMAT, value);
		snprintf(bufm, sizeof(bufm), INT64_FORMAT, max_value);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			  errmsg("%s value (%s) cannot be greater than MAXVALUE (%s)",
					 valname, bufs, bufm)));
	}

}

/*
 * It's reasonable to expect many sequence AMs to care only about
 * RESTART [WITH] option of ALTER SEQUENCE command, so we provide
 * this interface for convenience.
 * It is also useful for ALTER SEQUENCE USING.
 */
int64
sequence_get_restart_value(List *options, int64 default_value, bool *found)
{
	ListCell *opt;

	foreach(opt, options)
	{
		DefElem    *defel = (DefElem *) lfirst(opt);

		if (strcmp(defel->defname, "restart") == 0)
		{
			*found = true;
			if (defel->arg != NULL)
				return defGetInt64(defel);
			else
				return default_value;
		}
	}

	*found = false;
	return default_value;
}
