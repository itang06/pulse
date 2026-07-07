//! Generated Protobuf/gRPC types for the `pulse.v1` schema.
//!
//! Single Rust home for the wire contract: the SDK uses the client, the
//! gateway's Go equivalent lives in `gateway/gen`, and the processor/sink use
//! the message types to decode Kafka payloads.

pub mod pulse {
    pub mod v1 {
        tonic::include_proto!("pulse.v1");
    }
}

pub use pulse::v1::*;
