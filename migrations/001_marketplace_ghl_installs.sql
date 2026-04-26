-- Migration 001: GHL Marketplace OAuth install persistence
-- Creates the marketplace schema and ghl_installs table for iron-setter's
-- GHL Marketplace app. One row per installed GHL location.

CREATE SCHEMA IF NOT EXISTS marketplace;

CREATE TABLE IF NOT EXISTS marketplace.ghl_installs (
    location_id     VARCHAR(64)  PRIMARY KEY,
    company_id      VARCHAR(64),
    entity_id       UUID,            -- links to entities(id) when entity is created/linked
    access_token    TEXT         NOT NULL,
    refresh_token   TEXT         NOT NULL,
    expires_at      TIMESTAMPTZ  NOT NULL,
    scopes          TEXT         NOT NULL DEFAULT '',
    installed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_marketplace_ghl_installs_entity_id
    ON marketplace.ghl_installs (entity_id);
CREATE INDEX IF NOT EXISTS ix_marketplace_ghl_installs_expires_at
    ON marketplace.ghl_installs (expires_at);

COMMENT ON TABLE marketplace.ghl_installs IS
    'OAuth tokens from GHL Marketplace app installs. One row per location.';
