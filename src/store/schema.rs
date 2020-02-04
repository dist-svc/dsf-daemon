
use diesel::table;

table! {
    services (id) {
        id -> Text,
        index -> Integer,

        state -> Text,
        
        public_key -> Text,
        secret_key -> Nullable<Text>,
        
        last_updated -> Nullable<Timestamp>,
        subscribers -> Integer,
        replicas -> Integer,
        original -> Bool,
    }
}

table! {
    peers (id) {
        id -> Text,
        state -> Text,
        public_key -> Nullable<Text>,
        
        address -> Text,
        address_mode -> Text,

        last_seen -> Nullable<Timestamp>,

        sent -> Integer,
        received -> Integer,
        blocked -> Bool,
    }
}

table! {
    peer_addresses (id) {
        id -> Text,
        
        address -> Text,
        address_mode -> Text,

        last_used -> Nullable<Timestamp>,
    }
}

table! {
    data (id) {
        id -> Text,
        sig -> Text,

    }
}