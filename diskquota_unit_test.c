
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "cdb/cdbbufferedappend.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relfilenodemap.h"

#include "gp_activetable.h"
#include "diskquota.h"

PG_FUNCTION_INFO_V1(relation_size);

Datum
relation_size(PG_FUNCTION_ARGS)
{
    Oid tablespace = PG_GETARG_OID(0);
	Oid relfilenode = PG_GETARG_OID(1);
    int backend = PG_GETARG_BOOL(2) ? -2 : -1;
    RelFileNodeBackend rnode = {0};
    Size size = 0;

    rnode.node.dbNode = MyDatabaseId;
    rnode.node.relNode = relfilenode;
    rnode.node.spcNode = tablespace ? tablespace : DEFAULTTABLESPACE_OID;
    rnode.backend = backend;

    size = diskquota_get_relation_size_by_relfilenode(&rnode);

	PG_RETURN_INT64(size);
}
