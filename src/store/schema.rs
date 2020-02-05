
use diesel::table;

table! {
    services (service_id) {
        service_id -> Text,
        index -> Integer,

        state -> Text,

        public_key -> Text,
        secret_key -> Nullable<Text>,

        primary_page -> Nullable<Text>,
        replica_page -> Nullable<Text>,
        
        last_updated -> Nullable<Timestamp>,

        subscribers -> Integer,
        replicas -> Integer,
        original -> Bool,
    }
}

table! {
    peers (peer_id) {
        peer_id -> Text,
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
    peer_addresses (peer_id) {
        peer_id -> Text,
        
        address -> Text,
        address_mode -> Text,

        last_used -> Nullable<Timestamp>,
    }
}

table! {
    subscriptions (service_id, peer_id) {
        service_id -> Text,
        peer_id -> Text,

        last_updated -> Nullable<Timestamp>,
        expiry -> Nullable<Timestamp>,
    }
}


table! {
    objects (service_id) {
        service_id -> Text,
        object_index -> Integer,

        body -> Nullable<Blob>,
        raw -> Nullable<Blob>,

        parent -> Nullable<Text>,
        signature -> Text,
    }
}
