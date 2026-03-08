//! # tiders
//!
//! High-performance blockchain data indexing and decoding library.
//!
//! tiders provides a unified API for ingesting raw blockchain data from EVM (Ethereum)
//! and SVM (Solana) chains, decoding contract-specific data using ABI/IDL signatures,
//! and querying the results — all in Apache Arrow columnar format.
//!
//! ## Crate Organization
//!
//! - [`cast`] — Type casting and encoding/decoding (hex, base58, Decimal256) for Arrow columns.
//! - [`evm_decode`] — Decode EVM event logs and function calls using Solidity ABI signatures.
//! - [`evm_schema`] — Pre-built Arrow schemas for EVM data types (blocks, transactions, logs, traces).
//! - [`ingest`] — Stream blockchain data from multiple providers (HyperSync, SQD, RPC).
//! - [`query`] — Filter, join, and project Arrow RecordBatches.
//! - [`svm_decode`] — Decode Solana instructions and program logs using Borsh deserialization.
//! - [`svm_schema`] — Pre-built Arrow schemas for Solana data types (blocks, transactions, instructions, etc.).

pub use tiders_cast as cast;
pub use tiders_evm_decode as evm_decode;
pub use tiders_evm_schema as evm_schema;
pub use tiders_ingest as ingest;
pub use tiders_query as query;
pub use tiders_svm_decode as svm_decode;
pub use tiders_svm_schema as svm_schema;
#[cfg(test)]
mod tests;
