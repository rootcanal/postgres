/*-------------------------------------------------------------------------
 *
 * amcmds.c
 *	  Routines for SQL commands that manipulate access methods.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/amcmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/*
 * Convert a handler function name passed from the parser to an Oid. This
 * function either return valid function Oid or throw an error.
 */
static Oid
lookup_am_handler_func(List *handler_name, Oid handler_type)
{
	Oid			handlerOid;
	Oid			funcargtypes[1] = {INTERNALOID};

	if (handler_name == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("handler function is not specified")));

	/* handlers have no arguments */
	handlerOid = LookupFuncName(handler_name, 1, funcargtypes, false);

	/* check that handler has correct return type */
	if (get_func_rettype(handlerOid) != handler_type)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must return type \"%s\"",
						NameListToString(handler_name),
						format_type_be(handler_type))));

	return handlerOid;
}

/*
 * CreateAcessMethod
 *		Registers a new access method.
 */
ObjectAddress
CreateAccessMethod(CreateAmStmt *stmt)
{
	Relation		rel;
	ObjectAddress	myself;
	ObjectAddress	referenced;
	Oid				amoid;
	Oid				amhandler;
	bool			nulls[Natts_pg_am];
	Datum			values[Natts_pg_am];
	HeapTuple		tup;

	rel = heap_open(AccessMethodRelationId, RowExclusiveLock);

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			errmsg("permission denied to create access method \"%s\"",
				   stmt->amname),
			errhint("Must be superuser to create access method.")));

	/* Check if name is busy */
	amoid = GetSysCacheOid1(AMNAME, CStringGetDatum(stmt->amname));
	if (OidIsValid(amoid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("access method \"%s\" already exists", stmt->amname)));
	}

	/*
	 * Get handler function oid. Handler signature depends on access method
	 * type.
	 */
	switch(stmt->amtype)
	{
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("invalid access method type \"%c\"", stmt->amtype)));
			break;
	}

	/*
	 * Insert tuple into pg_am.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_am_amname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->amname));
	values[Anum_pg_am_amhandler - 1] = ObjectIdGetDatum(amhandler);
	values[Anum_pg_am_amtype - 1] = CharGetDatum(stmt->amtype);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	amoid = simple_heap_insert(rel, tup);
	CatalogUpdateIndexes(rel, tup);
	heap_freetuple(tup);

	myself.classId = AccessMethodRelationId;
	myself.objectId = amoid;
	myself.objectSubId = 0;

	/* Record dependecy on handler function */
	referenced.classId = ProcedureRelationId;
	referenced.objectId = amhandler;
	referenced.objectSubId = 0;

	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	recordDependencyOnCurrentExtension(&myself, false);

	heap_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Guts of access method deletion.
 */
void
RemoveAccessMethodById(Oid amOid)
{
	Relation	relation;
	HeapTuple	tup;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop access methods")));

	relation = heap_open(AccessMethodRelationId, RowExclusiveLock);

	tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for access method %u", amOid);

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

/*
 * Convert single charater access method type into string for error reporting.
 */
static char *
get_am_type_string(char amtype)
{
	switch (amtype)
	{
		default:
			elog(ERROR, "invalid access method type \"%c\"", amtype);
	}
}

/*
 * get_am_oid - given an access method name and type, look up the OID
 *
 * If missing_ok is false, throw an error if access method not found.  If
 * true, just return InvalidOid.
 */
Oid
get_am_oid(const char *amname, char amtype, bool missing_ok)
{
	HeapTuple	tup;
	Oid			oid = InvalidOid;

	tup = SearchSysCache1(AMNAME, CStringGetDatum(amname));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_am amform = (Form_pg_am) GETSTRUCT(tup);

		if (amform->amtype != amtype)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("access method \"%s\" type is not %s",
							NameStr(amform->amname),
							get_am_type_string(amtype))));

		oid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}

	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("access method \"%s\" does not exist", amname)));
	return oid;
}

/*
 * get_am_name - given an access method OID name and type, look up the name
 *
 * Access method type is not required for lookup.  However it's useful to check
 * the type to ensure it is what we're looking for.
 */
char *
get_am_name(Oid amOid)
{
	HeapTuple	tup;
	char	   *result = NULL;

	tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_am amform = (Form_pg_am) GETSTRUCT(tup);

		result = pstrdup(NameStr(amform->amname));
		ReleaseSysCache(tup);
	}
	return result;
}

