-- 0001: enable the timescaledb extension.
--
-- Sink tables are defined in the append-only migration 0002_sink.sql.
-- `make migrate` applies files in name order without a migration tracking table.

CREATE EXTENSION IF NOT EXISTS timescaledb;
