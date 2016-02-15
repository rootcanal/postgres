/*-------------------------------------------------------------------------
 *
 * replslotdesc.c
 *	  rmgr descriptor routines for replication/slot.c
 *
 * Portions Copyright (c) 2015, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/replslotdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/slot_xlog.h"

void
replslot_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_REPLSLOT_UPDATE:
			{
				ReplicationSlotInWAL xlrec;

				xlrec = (ReplicationSlotInWAL) rec;

				appendStringInfo(buf, "UPDATE of slot %s with restart %X/%X and xid %u confirmed to %X/%X",
						NameStr(xlrec->name),
						(uint32)(xlrec->restart_lsn>>32), (uint32)(xlrec->restart_lsn),
						xlrec->xmin,
						(uint32)(xlrec->confirmed_flush>>32), (uint32)(xlrec->confirmed_flush));

				break;
			}
		case XLOG_REPLSLOT_DROP:
			{
				xl_replslot_drop *xlrec;

				xlrec = (xl_replslot_drop *) rec;

				appendStringInfo(buf, "DROP of slot %s", NameStr(xlrec->name));

				break;
			}
	}
}

const char *
replslot_identify(uint8 info)
{
	switch (info)
	{
		case XLOG_REPLSLOT_UPDATE:
			return "UPDATE";
		case XLOG_REPLSLOT_DROP:
			return "DROP";
		default:
			return NULL;
	}
}
