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
	 * Slots created on master become failover-slots and are maintained
	 * on all standbys, but are only assignable after failover.
	 */
	bool		failover;

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
	TimeLineID	restart_tli;

	/* oldest LSN that the client has acked receipt for */
	XLogRecPtr	confirmed_flush;

	/* plugin name */
	NameData	plugin;
} ReplicationSlotPersistentData;

typedef ReplicationSlotPersistentData *ReplicationSlotInWAL;

#define SizeOfReplicationSlotInWAL (offsetof(ReplicationSlotPersistentData, plugin) + sizeof(NameData));

/*
 * WAL records for failover slots
 */
#define XLOG_REPLSLOT_UPDATE	0x00
#define XLOG_REPLSLOT_DROP		0x01

typedef struct xl_replslot_drop
{
	NameData	name;
} xl_replslot_drop;

#define SizeOfXLReplSlotDrop (offsetof(xl_replslot_drop, name) + sizeof(NameData));

/* WAL logging */
extern void replslot_redo(XLogRecPtr lsn, XLogRecord *record);
extern void replslot_desc(StringInfo buf, uint8 xl_info, char *rec);

#endif   /* SLOT_XLOG_H */
