/* -------------------------------------------------------------------------
 *
 * pg_utils.h
 *
 * This code is utils for detecting active table for databases
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 *
 * -------------------------------------------------------------------------
 */
#ifndef DISKQUOTA_GP_UTILS_H
#define DISKQUOTA_GP_UTILS_H

#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "storage/lock.h"

extern Size diskquota_get_relation_size_by_relfilenode(RelFileNodeBackend *rnode);
extern Relation diskquota_relation_open(Oid relid, LOCKMODE mode);

#endif //DISKQUOTA_GP_UTILS_H
