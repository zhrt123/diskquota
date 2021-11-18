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
#include "gp_utils.h"

HTAB	   *relation_cache = NULL;
HTAB	   *relid_cache = NULL;

static void get_relation_entry(Oid relid, DiskQuotaRelationCacheEntry* entry);
static void update_relation_entry(Oid relid, DiskQuotaRelationCacheEntry *relation_entry, DiskQuotaRelidCacheEntry *relid_entry);
static RelFileNodeBackend get_relfilenode_by_relid(Oid relid);

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

static Size
do_calculate_table_size(DiskQuotaRelationCacheEntry *entry)
{
	Size tablesize = 0;
	RelFileNodeBackend rnode;
	Oid subrelid;
	int i;

	rnode = get_relfilenode_by_relid(entry->relid);
	tablesize += diskquota_get_relation_size_by_relfilenode(&rnode);

	for (i = 0; i < entry->subrel_num; i++)
	{
		subrelid = entry->subrel_oid[i];
		rnode = get_relfilenode_by_relid(subrelid);
		tablesize += diskquota_get_relation_size_by_relfilenode(&rnode);
	}
	return tablesize;
}

Size
calculate_table_size(Oid relid)
{
    DiskQuotaRelationCacheEntry entry = {0};

	get_relation_entry(relid, &entry);

	return do_calculate_table_size(&entry);
}

static void
get_relation_entry(Oid relid, DiskQuotaRelationCacheEntry* entry)
{
	DiskQuotaRelationCacheEntry* tentry;

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	tentry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
	if (tentry)
	{
		memcpy(entry, tentry, sizeof(DiskQuotaRelationCacheEntry));
		LWLockRelease(diskquota_locks.relation_cache_lock);
		return;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);
	
	update_relation_entry(relid, entry, NULL);
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

RelFileNodeBackend
get_relfilenode_by_relid(Oid relid)
{
	DiskQuotaRelationCacheEntry *relation_cache_entry;
	RelFileNodeBackend rnode = {0};
	Relation rel;
	
	rel = try_relation_open(relid, NoLock, false);
	if (rel)
	{
		rnode.node = rel->rd_node;
		rnode.backend = rel->rd_backend;
		relation_close(rel, NoLock);
		
		remove_cache_entry(relid, InvalidOid);
		return rnode;
	}

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	relation_cache_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
	if (relation_cache_entry)
	{
		rnode = relation_cache_entry->rnode;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return rnode;
}

void
remove_cache_entry_recursion_wio_lock(Oid relid)
{
	DiskQuotaRelationCacheEntry *relation_entry;
	int i;

	if (OidIsValid(relid))
	{
		relation_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
		if (relation_entry)
		{
			for (i = 0; i < relation_entry->subrel_num; i++)
			{
				remove_cache_entry_recursion_wio_lock(relation_entry->subrel_oid[i]);
			}
			hash_search(relid_cache, &relation_entry->rnode.node.relNode, HASH_REMOVE, NULL);
			hash_search(relation_cache, &relid, HASH_REMOVE, NULL);
		}
	}
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

bool
get_table_commit_status(Oid relid)
{
	Relation rel = try_relation_open(relid, NoLock, false);
	if (rel)
	{
		relation_close(rel, NoLock);
		return true;
	}
	return false;
}

static void
add_subrel_to_relation_cache_entry(DiskQuotaRelationCacheEntry *entry, Oid relid)
{
	int i;
	
	for (i = 0; i < entry->subrel_num; i++)
	{
		if (entry->subrel_oid[i] == relid)
		{
			return;
		}
	}
	entry->subrel_oid[entry->subrel_num++] = relid;
}
static void
update_subtable_relation_entry(Oid relid, DiskQuotaRelationCacheEntry *pentry)
{
	bool found;
	Relation rel;
	DiskQuotaRelationCacheEntry *entry;
	DiskQuotaRelidCacheEntry *relid_entry;
	
	rel = diskquota_relation_open(relid, NoLock);
	if (rel == NULL)
	{
		return;
	}

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	entry = hash_search(relation_cache, &relid, HASH_FIND, &found);
	if (found)
	{
		entry->relid = relid;
		entry->rnode.node = rel->rd_node;
		entry->rnode.backend = rel->rd_backend;
		entry->owneroid = rel->rd_rel->relowner;
		entry->namespaceoid = rel->rd_rel->relnamespace;
		entry->primary_table_relid = pentry->relid;
	}

	relid_entry = hash_search(relid_cache, &rel->rd_node.relNode, HASH_FIND, &found);
	if (found)
	{
		relid_entry->relfilenode = rel->rd_node.relNode;
		relid_entry->relid = relid;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	add_subrel_to_relation_cache_entry(pentry, relid);

	if (rel->rd_rel && rel->rd_rel->relhasindex)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *cell;
		foreach(cell, index_oids)
		{
			Oid	idxOid = lfirst_oid(cell);
			update_subtable_relation_entry(idxOid, pentry);
		}


		list_free(index_oids);
	}

	relation_close(rel, NoLock);
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

	if (rel->rd_rel->relkind == 'i' && rel->rd_rel->relnamespace != PG_AOSEGMENT_NAMESPACE 
									&& rel->rd_rel->relnamespace != PG_TOAST_NAMESPACE 
									&& rel->rd_rel->relnamespace != PG_BITMAPINDEX_NAMESPACE)
	{
		relation_entry->primary_table_relid = relid;
	}

	if (rel->rd_rel->relkind == 'r' || rel->rd_rel->relkind == 'm')
	{
		relation_entry->primary_table_relid = relid;

		// toast table
		if (OidIsValid(rel->rd_rel->reltoastrelid))
		{
			update_subtable_relation_entry(rel->rd_rel->reltoastrelid, relation_entry);
		}

		// ao table
		if (RelationIsAppendOptimized(rel) && rel->rd_appendonly != NULL && relation_entry->subrel_num == 0)
		{
			if (OidIsValid(rel->rd_appendonly->segrelid))
			{
				update_subtable_relation_entry(rel->rd_appendonly->segrelid, relation_entry);
			}

			/* block directory may not exist, post upgrade or new table that never has indexes */
			if (OidIsValid(rel->rd_appendonly->blkdirrelid))
			{
				update_subtable_relation_entry(rel->rd_appendonly->blkdirrelid, relation_entry);
			}
			if (OidIsValid(rel->rd_appendonly->visimaprelid))
			{
				update_subtable_relation_entry(rel->rd_appendonly->visimaprelid, relation_entry);
			}
		}
	}

	relation_close(rel, NoLock);
}

void
update_relation_cache(Oid relid)
{
	DiskQuotaRelationCacheEntry relation_entry_data = {0};
	DiskQuotaRelationCacheEntry *relation_entry;
	DiskQuotaRelidCacheEntry relid_entry_data = {0};
	DiskQuotaRelidCacheEntry *relid_entry;

	update_relation_entry(relid, &relation_entry_data, &relid_entry_data);

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	relation_entry = hash_search(relation_cache, &relation_entry_data.relid, HASH_ENTER, NULL);
	memcpy(relation_entry, &relation_entry_data, sizeof(DiskQuotaRelationCacheEntry));

	relid_entry = hash_search(relid_cache, &relid_entry_data.relfilenode, HASH_ENTER, NULL);
	memcpy(relid_entry, &relid_entry_data, sizeof(DiskQuotaRelidCacheEntry));
	LWLockRelease(diskquota_locks.relation_cache_lock);
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
	Oid prelid = relid;

	if (get_table_commit_status(relid) == true)
	{
		remove_cache_entry(relid, InvalidOid);
		prelid = get_primary_table_oid_by_relname(relid);
		return prelid;
	}

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	relation_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
	if (relation_entry)
	{
		prelid = relation_entry->relid;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return prelid;
}