-- 002_payload_hardening.sql
-- Hardens serper_places JSON storage & parsing.
-- Replace {{PROJECT}} / {{DATASET}} before running (see validation commands).

ALTER TABLE `{{PROJECT}}.{{DATASET}}.serper_places`
ADD COLUMN IF NOT EXISTS payload_raw STRING;

ALTER TABLE `{{PROJECT}}.{{DATASET}}.serper_places`
ALTER COLUMN payload DROP NOT NULL;

ALTER TABLE `{{PROJECT}}.{{DATASET}}.serper_places`
ALTER COLUMN payload SET OPTIONS (description="Parsed JSON payload (NULL if parse failed)");

ALTER TABLE `{{PROJECT}}.{{DATASET}}.serper_places`
ALTER COLUMN payload_raw SET OPTIONS (description="Original JSON text as received");
