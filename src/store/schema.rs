
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
    }
}

table! {
    data (id) {
        id -> Text,
    }
}