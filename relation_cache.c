#include "postgres.h"

#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/objectaccess.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relfilenodemap.h"


#include "relation_cache.h"
#include "diskquota.h"

HTAB	   *relation_cache = NULL;
HTAB	   *relid_cache = NULL;

static void update_relation_entry(Oid relid, DiskQuotaRelationCacheEntry *relation_entry, DiskQuotaRelidCacheEntry *relid_entry);

void
init_shm_worker_relation_cache(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaRelationCacheEntry);
	ctl.hash = tag_hash;

	relation_cache = ShmemInitHash("relation_cache",
									  diskquota_max_active_tables,
									  diskquota_max_active_tables,
									  &ctl,
									  HASH_ELEM | HASH_FUNCTION);

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaRelidCacheEntry);
	ctl.hash = tag_hash;

	relid_cache = ShmemInitHash("relid_cache",
									  diskquota_max_active_tables,
									  diskquota_max_active_tables,
									  &ctl,
									  HASH_ELEM | HASH_FUNCTION);
}

Oid
get_relid_by_relfilenode(RelFileNode relfilenode)
{
	Oid relid;

	relid = RelidByRelfilenode(relfilenode.spcNode, relfilenode.relNode);
	if(OidIsValid(relid))
	{
		remove_cache_entry(InvalidOid, relfilenode.relNode);
		return relid;
	}

	relid = get_uncommitted_table_relid(relfilenode.relNode);
	return relid;
}

void
remove_cache_entry(Oid relid, Oid relfilenode)
{
	DiskQuotaRelationCacheEntry *relation_entry;
	DiskQuotaRelidCacheEntry *relid_entry;

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	if (OidIsValid(relid))
	{
		relation_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
		if (relation_entry)
		{
			hash_search(relid_cache, &relation_entry->rnode.node.relNode, HASH_REMOVE, NULL);
			hash_search(relation_cache, &relid, HASH_REMOVE, NULL);
		}
	}

	if (OidIsValid(relfilenode))
	{
		relid_entry = hash_search(relid_cache, &relfilenode, HASH_FIND, NULL);
		if (relid_entry)
		{
			hash_search(relation_cache, &relid_entry->relid, HASH_REMOVE, NULL);
			hash_search(relid_cache, &relfilenode, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);
}

Oid
get_uncommitted_table_relid(Oid relfilenode)
{
	Oid relid = InvalidOid;
	DiskQuotaRelidCacheEntry *entry;
	
	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	entry = hash_search(relid_cache, &relfilenode, HASH_FIND, NULL);
	if (entry)
	{
		relid = entry->relid;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return relid;
}

static void
add_auxrelid_to_relation_entry(DiskQuotaRelationCacheEntry *entry, Oid relid)
{
	int i;
	
	for (i = 0; i < entry->auxrel_num; i++)
	{
		if (entry->auxrel_oid[i] == relid)
		{
			return;
		}
	}
	entry->auxrel_oid[entry->auxrel_num++] = relid;
}

static void
update_relation_entry(Oid relid, DiskQuotaRelationCacheEntry *relation_entry, DiskQuotaRelidCacheEntry *relid_entry)
{
	Relation rel;

	rel = diskquota_relation_open(relid, NoLock);
	if (rel == NULL)
	{
		return;
	}

	if (relation_entry)
	{
		relation_entry->relid = relid;
		relation_entry->rnode.node = rel->rd_node;
		relation_entry->rnode.backend = rel->rd_backend;
		relation_entry->owneroid = rel->rd_rel->relowner;
		relation_entry->namespaceoid = rel->rd_rel->relnamespace;
	}

	if (relid_entry)
	{
		relid_entry->relfilenode = rel->rd_node.relNode;
		relid_entry->relid = relid;
	}

	relation_entry->primary_table_relid = relid;

	relation_close(rel, NoLock);
}

void
update_relation_cache(Oid relid)
{
	DiskQuotaRelationCacheEntry relation_entry_data = {0};
	DiskQuotaRelationCacheEntry *relation_entry;
	DiskQuotaRelidCacheEntry relid_entry_data = {0};
	DiskQuotaRelidCacheEntry *relid_entry;
	Oid prelid;

	update_relation_entry(relid, &relation_entry_data, &relid_entry_data);

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	relation_entry = hash_search(relation_cache, &relation_entry_data.relid, HASH_ENTER, NULL);
	memcpy(relation_entry, &relation_entry_data, sizeof(DiskQuotaRelationCacheEntry));

	relid_entry = hash_search(relid_cache, &relid_entry_data.relfilenode, HASH_ENTER, NULL);
	memcpy(relid_entry, &relid_entry_data, sizeof(DiskQuotaRelidCacheEntry));
	LWLockRelease(diskquota_locks.relation_cache_lock);

	prelid = get_primary_table_oid(relid);
	if (OidIsValid(prelid) && prelid != relid)
	{
		LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
		relation_entry->primary_table_relid = prelid;
		relation_entry = hash_search(relation_cache, &prelid, HASH_FIND, NULL);
		if (relation_entry)
		{
			add_auxrelid_to_relation_entry(relation_entry, relid);
		}
		LWLockRelease(diskquota_locks.relation_cache_lock);
	}
}

static Oid
get_primary_table_oid_by_relname(Oid relid)
{
	Relation rel;
	Oid namespace;
	char relname[NAMEDATALEN];

	rel = diskquota_relation_open(relid, NoLock);
	if (rel == NULL)
	{
		return InvalidOid;
	}

	namespace = rel->rd_rel->relnamespace;
	memcpy(relname, rel->rd_rel->relname.data, NAMEDATALEN);
	relation_close(rel, NoLock);

	switch (namespace)
	{
		case PG_TOAST_NAMESPACE:
			if (strncmp(relname, "pg_toast", 8) == 0)
				return atoi(&relname[9]);
		break;
		case PG_AOSEGMENT_NAMESPACE:
		{
			if (strncmp(relname, "pg_aoseg", 8) == 0)
				return atoi(&relname[9]);
			else if (strncmp(relname, "pg_aovisimap", 12) == 0)
				return atoi(&relname[13]);
		}
		break;
	}
	return relid;
}

Oid
get_primary_table_oid(Oid relid)
{
	DiskQuotaRelationCacheEntry *relation_entry;
	Oid cached_prelid = relid;
	Oid parsed_prelid;

	parsed_prelid = get_primary_table_oid_by_relname(relid);
	if (OidIsValid(parsed_prelid))
	{
		return parsed_prelid;
	}

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	relation_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
	if (relation_entry)
	{
		cached_prelid = relation_entry->primary_table_relid;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return cached_prelid;
}

void
remove_committed_relation_from_cache(void)
{
	HeapTuple	tuple;
	HeapScanDesc relScan;
	Relation	classRel;
	Oid relid;
	List *oid_list = NIL;
	ListCell *l;

	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);

		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;
		relid = HeapTupleGetOid(tuple);
		lappend_oid(oid_list, relid);
	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

	/* use oid_list to avoid hold relation_cache_lock and AccessShareLock of pg_class */
	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	foreach(l, oid_list)
	{
		relid = lfirst_oid(l);
		remove_cache_entry(relid, InvalidOid);
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	list_free(oid_list);
}