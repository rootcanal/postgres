CREATE EXTENSION failover_slots;

CREATE VIEW public.stable_replslots_cols AS
SELECT slot_name, plugin, slot_type, "database", active, failover
FROM pg_catalog.pg_replication_slots_failover()
ORDER BY slot_name;

SELECT * FROM stable_replslots_cols;

SELECT slot_name FROM pg_create_logical_replication_slot('default', 'test_decoding');
SELECT slot_name FROM pg_create_logical_replication_slot('nonfailover', 'test_decoding', false);
SELECT slot_name FROM pg_create_logical_replication_slot('failover', 'test_decoding', true);

SELECT slot_name FROM pg_create_physical_replication_slot('default_p');
SELECT slot_name FROM pg_create_physical_replication_slot('nonfailover_p', false, false);
SELECT slot_name FROM pg_create_physical_replication_slot('failover_p', false,  true);

SELECT * FROM stable_replslots_cols;

CREATE TABLE some_table(
	id serial primary key,
	dummy integer
);

INSERT INTO some_table(dummy)
SELECT x
FROM generate_series(1,10) x;

SELECT data FROM pg_logical_slot_get_changes('failover', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

SELECT * FROM stable_replslots_cols;

SELECT data FROM pg_logical_slot_get_changes('nonfailover', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

SELECT * FROM stable_replslots_cols;

SELECT pg_drop_replication_slot('default');
SELECT pg_drop_replication_slot('default_p');
SELECT pg_drop_replication_slot('nonfailover');
SELECT pg_drop_replication_slot('nonfailover_p');
SELECT pg_drop_replication_slot('failover');
SELECT pg_drop_replication_slot('failover_p');

SELECT * FROM stable_replslots_cols;
