/*-------------------------------------------------------------------------
 * slot_xlog.h
 *	   Replication slot management.
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * src/include/replication/slot_xlog.h
 *-------------------------------------------------------------------------
 */
#ifndef SLOT_XLOG_H
#define SLOT_XLOG_H

#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"

/*
 * Behaviour of replication slots, upon release or crash.
 *
 * Slots marked as PERSISTENT are crashsafe and will not be dropped when
 * released. Slots marked as EPHEMERAL will be dropped when released or after
 * restarts.
 *
 * EPHEMERAL slots can be made PERSISTENT by calling ReplicationSlotPersist().
 */
typedef enum ReplicationSlotPersistency
{
	RS_PERSISTENT,
	RS_EPHEMERAL
} ReplicationSlotPersistency;

/*
 * On-Disk data of a replication slot, preserved across restarts.
 */
typedef struct ReplicationSlotPersistentData
{
	/* The slot's identifier */
	NameData	name;

	/* database the slot is active on */
	Oid			database;

	/*
	 * The slot's behaviour when being dropped (or restored after a crash).
	 */
	ReplicationSlotPersistency persistency;

	/*
	 * xmin horizon for data
	 *
	 * NB: This may represent a value that hasn't been written to disk yet;
	 * see notes for effective_xmin, below.
	 */
	TransactionId xmin;

	/*
	 * xmin horizon for catalog tuples
	 *
	 * NB: This may represent a value that hasn't been written to disk yet;
	 * see notes for effective_xmin, below.
	 */
	TransactionId catalog_xmin;

	/* oldest LSN that might be required by this replication slot */
	XLogRecPtr	restart_lsn;

	/*
	 * Oldest LSN that the client has acked receipt for.  This is used as the
	 * start_lsn point in case the client doesn't specify one, and also as a
	 * safety measure to jump forwards in case the client specifies a
	 * start_lsn that's further in the past than this value.
	 */
	XLogRecPtr	confirmed_flush;

	/* plugin name */
	NameData	plugin;

	/*
	 * Slots created on master become failover-slots and are maintained
	 * on all standbys, but are only assignable after failover.
	 */
	bool		failover;
} ReplicationSlotPersistentData;

typedef ReplicationSlotPersistentData *ReplicationSlotInWAL;

/*
 * WAL records for failover slots
 */
#define XLOG_REPLSLOT_UPDATE	0x10
#define XLOG_REPLSLOT_DROP		0x20

typedef struct xl_replslot_drop
{
	NameData	name;
} xl_replslot_drop;

/* WAL logging */
extern void replslot_redo(XLogReaderState *record);
extern void replslot_desc(StringInfo buf, XLogReaderState *record);
extern const char *replslot_identify(uint8 info);

#endif   /* SLOT_XLOG_H */
