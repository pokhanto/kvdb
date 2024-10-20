use serde::{Deserialize, Serialize};

/// Request enum
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Get variant
    Get {
        /// Get variant key
        key: String,
    },
    /// Set variant
    Set {
        /// Set variant key
        key: String,
        /// Set variant value
        value: String,
    },
    /// Remove variant
    Remove {
        /// Remove variant key
        key: String,
    },
}

/// Response enum
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Ok
    Ok,
    /// Ok with value
    Value {
        /// Ok with value value
        value: Option<String>,
    },
    /// Error variant
    Error {
        /// Error variant message
        message: String,
    },
}
