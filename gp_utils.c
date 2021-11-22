/* -------------------------------------------------------------------------
 *
 * pg_utils.c
 *
 * This code is utils for detecting active table for databases
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/indexing.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "utils/fmgroids.h"
#include "access/htup_details.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "gp_utils.h"

#include <sys/stat.h>

Size diskquota_get_relation_size_by_relfilenode(RelFileNodeBackend *rnode);

/*
 * calculate size of (one fork of) a table in transaction
 * This function is following calculate_relation_size()
 */
Size
diskquota_get_relation_size_by_relfilenode(RelFileNodeBackend *rnode)
{
    int64       totalsize = 0;
    ForkNumber  forkNum;
    int64       size = 0;
    char       *relationpath;
    char        pathname[MAXPGPATH];
    unsigned int segcount = 0;

    PG_TRY();
	{
        for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
        {
            relationpath = relpathbackend(rnode->node, rnode->backend, forkNum);
            size = 0;

            for (segcount = 0;; segcount++)
            {
                struct stat fst;

                CHECK_FOR_INTERRUPTS();

                if (segcount == 0)
                    snprintf(pathname, MAXPGPATH, "%s",
                            relationpath);
                else
                    snprintf(pathname, MAXPGPATH, "%s.%u",
                            relationpath, segcount);

                if (stat(pathname, &fst) < 0)
                {
                    if (errno == ENOENT)
                        break;
                }
                size += fst.st_size;
            }

            totalsize += size;
        }
    }
	PG_CATCH();
	{
		HOLD_INTERRUPTS();
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

    return totalsize;
}

Relation
diskquota_relation_open(Oid relid, LOCKMODE mode)
{
	Relation rel;
	bool success_open = false;
    int32 SavedInterruptHoldoffCount = InterruptHoldoffCount;

	PG_TRY();
	{
		rel = relation_open(relid, mode);
		success_open = true;
	}
	PG_CATCH();
	{
        InterruptHoldoffCount = SavedInterruptHoldoffCount;
		HOLD_INTERRUPTS();
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	return success_open ? rel : NULL;
}

List*
diskquota_get_index_list(Oid relid)
{
    Relation	indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result;
	Oid			oidIndex = InvalidOid;

	result = NIL;
	oidIndex = InvalidOid;

	/* Prepare to scan pg_index for entries having indrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				relid);

	indrel = heap_open(IndexRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		Form_pg_index index = (Form_pg_index) GETSTRUCT(htup);

		/*
		 * Ignore any indexes that are currently being dropped.  This will
		 * prevent them from being searched, inserted into, or considered in
		 * HOT-safety decisions.  It's unsafe to touch such an index at all
		 * since its catalog entries could disappear at any instant.
		 */
		if (!IndexIsLive(index))
			continue;

		/* Add index's OID to result list in the proper order */
		result = lappend_oid(result, index->indexrelid);
	}

	systable_endscan(indscan);

	heap_close(indrel, AccessShareLock);

	return result;
}