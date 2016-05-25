\echo Use "CREATE EXTENSION failover_slots" to load this file. \quit

CREATE FUNCTION
pg_catalog.pg_create_physical_replication_slot(
	    IN slot_name name, IN immediately_reserve boolean,
	    IN failover boolean, OUT slot_name name,
	    OUT xlog_position pg_lsn)
RETURNS RECORD
LANGUAGE INTERNAL
STRICT VOLATILE
AS 'pg_create_physical_replication_slot';

CREATE FUNCTION
pg_catalog.pg_create_logical_replication_slot(
	    IN slot_name name, IN plugin name, IN failover boolean,
	    OUT slot_name text, OUT xlog_position pg_lsn)
RETURNS RECORD
LANGUAGE INTERNAL
STRICT VOLATILE
AS 'pg_create_logical_replication_slot';

CREATE FUNCTION
pg_catalog.pg_get_replication_slots_failover(
    OUT slot_name name, OUT plugin name, OUT slot_type text, OUT datoid oid,
    OUT active boolean, OUT active_pid integer, OUT xmin xid, OUT catalog_xmin xid,
    OUT restart_lsn pg_lsn, OUT confirmed_flush_lsn pg_lsn, OUT failover boolean)
RETURNS SETOF record
LANGUAGE internal
STABLE ROWS 10
AS 'pg_get_replication_slots';

--
-- This should be a view, but we can't create views in
-- pg_catalog without running afoul of allow_system_table_mods.
--
-- Functions are permitted.
--
CREATE FUNCTION pg_catalog.pg_replication_slots_failover()
RETURNS TABLE (
    slot_name name,
    plugin name,
    slot_type text,
    datoid oid,
    "database" name,
    active boolean,
    active_pid integer,
    xmin xid,
    catalog_xmin xid,
    restart_lsn pg_lsn,
    confirmed_flush_lsn pg_lsn,
    failover boolean
) AS $$
    SELECT
            L.slot_name,
            L.plugin,
            L.slot_type,
            L.datoid,
            D.datname AS database,
            L.active,
            L.active_pid,
            L.xmin,
            L.catalog_xmin,
            L.restart_lsn,
            L.confirmed_flush_lsn,
            L.failover
    FROM pg_get_replication_slots_failover() AS L
            LEFT JOIN pg_database D ON (L.datoid = D.oid);
$$ LANGUAGE sql;
