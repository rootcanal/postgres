/*-------------------------------------------------------------------------
 *
 * message.c
 *	  Generic logical messages.
 *
 * Copyright (c) 2013-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/message.c
 *
 * NOTES
 *
 * Generic logical messages allow XLOG logging of arbitrary binary blobs that
 * get passed to the logical decoding plugin. In normal XLOG processing they
 * are same as NOOP.
 *
 * These messages can be either transactional or non-transactional.
 * Transactional messages are part of current transaction and will be sent to
 * decoding plugin using in a same way as DML operations.
 * Non-transactional messages are sent to the plugin at the time when the
 * logical decoding reads them from XLOG.
 *
 * Every message carries prefix to avoid conflicts between different decoding
 * plugins. The prefix has to be registered before the message using that
 * prefix can be written to XLOG. The prefix can be registered exactly once to
 * avoid situation where multiple third party extensions try to use same
 * prefix.
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"

#include "catalog/indexing.h"

#include "nodes/execnodes.h"

#include "replication/message.h"
#include "replication/logical.h"

#include "utils/memutils.h"

/* List of registered logical message prefixes. */
static List	   *LogicalMsgPrefixList = NIL;

XLogRecPtr
LogLogicalMessage(const char *prefix, const char *message, size_t size,
				  bool transactional)
{
	ListCell		   *lc;
	bool				found = false;
	xl_logical_message	xlrec;

	/* Check if the provided prefix is known to us. */
	foreach(lc, LogicalMsgPrefixList)
	{
		char	*mp = lfirst(lc);

		if (strcmp(prefix, mp) == 0)
		{
			found = true;
			break;
		}
	}
	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("standby message prefix \"%s\" is not registered",
						prefix)));

	/*
	 * Force xid to be allocated if we're sending a transactional message.
	 */
	if (transactional)
	{
		Assert(IsTransactionState());
		GetCurrentTransactionId();
	}

	xlrec.transactional = transactional;
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.message_size = size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalMessage);
	XLogRegisterData((char *) prefix, xlrec.prefix_size);
	XLogRegisterData((char *) message, size);

	return XLogInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE);
}

void
RegisterLogicalMsgPrefix(const char *prefix)
{
	ListCell	   *lc;
	bool			found = false;
	MemoryContext	oldcxt;

	/* Check for duplicit registrations. */
	foreach(lc, LogicalMsgPrefixList)
	{
		char	*mp = lfirst(lc);

		if (strcmp(prefix, mp) == 0)
		{
			found = true;
			break;
		}
	}
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("logical decoding message prefix \"%s\" is already registered",
						prefix)));

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	LogicalMsgPrefixList = lappend(LogicalMsgPrefixList, pstrdup(prefix));
	MemoryContextSwitchTo(oldcxt);
}

void
logicalmsg_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info != XLOG_LOGICAL_MESSAGE)
			elog(PANIC, "logicalmsg_redo: unknown op code %u", info);

	/* This is only interesting for logical decoding, see decode.c. */
}
