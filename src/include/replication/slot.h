/*-------------------------------------------------------------------------
 * slot.h
 *	   Replication slot management.
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOT_H
#define SLOT_H

#include "fmgr.h"
#include "access/xlog.h"
#include "replication/slot_xlog.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Shared memory state of a single replication slot.
 */
typedef struct ReplicationSlot
{
	/* lock, on same cacheline as effective_xmin */
	slock_t		mutex;

	/* is this slot defined */
	bool		in_use;

	/* Who is streaming out changes for this slot? 0 in unused slots. */
	pid_t		active_pid;

	/* any outstanding modifications? */
	bool		just_dirtied;
	bool		dirty;

	/*
	 * For logical decoding, it's extremely important that we never remove any
	 * data that's still needed for decoding purposes, even after a crash;
	 * otherwise, decoding will produce wrong answers.  Ordinary streaming
	 * replication also needs to prevent old row versions from being removed
	 * too soon, but the worst consequence we might encounter there is
	 * unwanted query cancellations on the standby.  Thus, for logical
	 * decoding, this value represents the latest xmin that has actually been
	 * written to disk, whereas for streaming replication, it's just the same
	 * as the persistent value (data.xmin).
	 */
	TransactionId effective_xmin;
	TransactionId effective_catalog_xmin;

	/* data surviving shutdowns and crashes */
	ReplicationSlotPersistentData data;

	/* is somebody performing io on this slot? */
	LWLock	   *io_in_progress_lock;

	/* all the remaining data is only used for logical slots */

	/* ----
	 * When the client has confirmed flushes >= candidate_xmin_lsn we can
	 * advance the catalog xmin, when restart_valid has been passed,
	 * restart_lsn can be increased.
	 * ----
	 */
	TransactionId candidate_catalog_xmin;
	XLogRecPtr	candidate_xmin_lsn;
	XLogRecPtr	candidate_restart_valid;
	XLogRecPtr	candidate_restart_lsn;
} ReplicationSlot;

/*
 * Shared memory control area for all of replication slots.
 */
typedef struct ReplicationSlotCtlData
{
	/*
	 * This array should be declared [FLEXIBLE_ARRAY_MEMBER], but for some
	 * reason you can't do that in an otherwise-empty struct.
	 */
	ReplicationSlot replication_slots[1];
} ReplicationSlotCtlData;

/*
 * Pointers to shared memory
 */
extern ReplicationSlotCtlData *ReplicationSlotCtl;
extern ReplicationSlot *MyReplicationSlot;

/* GUCs */
extern PGDLLIMPORT int max_replication_slots;

/* shmem initialization functions */
extern Size ReplicationSlotsShmemSize(void);
extern void ReplicationSlotsShmemInit(void);

/* management of individual slots */
extern void ReplicationSlotCreate(const char *name, bool db_specific,
					  ReplicationSlotPersistency p, bool failover);
extern void ReplicationSlotPersist(void);
extern void ReplicationSlotDrop(const char *name);

extern void ReplicationSlotAcquire(const char *name);
extern void ReplicationSlotRelease(void);
extern void ReplicationSlotSave(void);
extern void ReplicationSlotMarkDirty(void);

/* misc stuff */
extern bool ReplicationSlotValidateName(const char *name, int elevel);
extern void ReplicationSlotReserveWal(void);
extern void ReplicationSlotsUpdateRequiredXmin(bool already_locked);
extern void ReplicationSlotsUpdateRequiredLSN(void);
extern XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void);
extern XLogRecPtr ReplicationSlotsComputeRequiredLSN(bool failover_only);
extern bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive);
extern void ReplicationSlotsDropDBSlots(Oid dboid);

extern void StartupReplicationSlots(bool drop_nonfailover_slots);
extern void CheckPointReplicationSlots(void);

extern void CheckSlotRequirements(void);

/* SQL callable functions */
extern Datum pg_create_physical_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_drop_replication_slot(PG_FUNCTION_ARGS);
extern Datum pg_get_replication_slots(PG_FUNCTION_ARGS);

#endif   /* SLOT_H */
