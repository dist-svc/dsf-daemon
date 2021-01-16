-- Add migration script here

CREATE TABLE IF NOT EXISTS services (
    "id" TEXT NOT NULL,
    "index" INT NOT NULL,
    "state" TEXT NOT NULL,

    public_key TEXT,
    private_key TEXT,
    secret_key TEXT,

    last_updated DATETIME,

    primary_page TEXT,
    replica_page TEXT,

    subscribers INT,
    replicas INT,
    origin BOOLEAN,
    subscribed BOOLEAN
);

CREATE TABLE IF NOT EXISTS peers (
    n INT NOT NULL,
    id TEXT NOT NULL,

    st TEXT NOT NULL,
    public_key TEXT,

    addr TEXT,
    addr_mode TEXT,

    seen DATETIME,

    tx_count INT,
    rx_count INT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    n INT NOT NULL,

    service_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,

    updated DATETIME,
    expiry DATETIME
);

CREATE TABLE IF NOT EXISTS pages (
    service_id TEXT NOT NULL,
    
    sig TEXT NOT NULL,

    last_sig TEXT
);
