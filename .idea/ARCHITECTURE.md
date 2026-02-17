# cherry-core Architecture Overview

## Project Structure

**cherry-core** is a high-performance Rust workspace providing core libraries for the `cherry` blockchain data pipeline framework. It handles data ingestion, decoding, querying, and type casting for both EVM (Ethereum) and SVM (Solana) blockchains, with all data represented as Apache Arrow RecordBatches.

### Workspace Organization

The project is a Cargo workspace with 9 crates:

#### 1. **core/** - Aggregator Crate
[core/Cargo.toml](core/Cargo.toml) - Re-exports all other crates under a single `cherry-core` umbrella.

[core/src/lib.rs](core/src/lib.rs) simply re-exports:
- `cherry_cast` as `cast`
- `cherry_evm_decode` as `evm_decode`
- `cherry_evm_schema` as `evm_schema`
- `cherry_ingest` as `ingest`
- `cherry_query` as `query`
- `cherry_svm_decode` as `svm_decode`
- `cherry_svm_schema` as `svm_schema`

#### 2. **evm-decode/** - EVM Event & Call Decoding
[evm-decode/Cargo.toml](evm-decode/Cargo.toml) - Decodes EVM event logs and function call data from raw binary into Arrow RecordBatches.

**Key Dependencies:**
- **alloy-dyn-abi**, **alloy-json-abi**: ABI parsing and dynamic decoding
- **arrow**: Output format for all decoded data

**Public API ([evm-decode/src/lib.rs](evm-decode/src/lib.rs)):**
- `decode_events()`: Decodes event log data (topics + body) using an event signature string
- `decode_call_inputs()` / `decode_call_outputs()`: Decodes function call input/output data
- `event_signature_to_arrow_schema()`: Generates Arrow schema from an event signature
- `function_signature_to_arrow_schemas()`: Generates input/output Arrow schemas from a function signature
- `signature_to_topic0()`: Computes topic0 selector from an event signature

**Internal Design:**
- Solidity types are mapped to Arrow types via `to_arrow_dtype()` (e.g., `bool` -> `Boolean`, `address` -> `Binary`, `uint256` -> `Decimal256(76,0)`)
- Decoded `DynSolValue` trees are recursively converted to Arrow arrays via `to_arrow()`
- Supports arbitrary nesting with Lists (arrays) and Structs (tuples)
- `allow_decode_fail` flag controls whether decode errors produce nulls or hard errors

#### 3. **evm-schema/** - EVM Schema Definitions
[evm-schema/Cargo.toml](evm-schema/Cargo.toml) - Defines canonical Arrow schemas and builders for EVM data tables.

**Public API ([evm-schema/src/lib.rs](evm-schema/src/lib.rs)):**
- `blocks_schema()` / `BlocksBuilder`: Block header fields (number, hash, gas, withdrawals, L1 fields, etc.)
- `transactions_schema()` / `TransactionsBuilder`: Transaction fields including receipt data, L1/L2 fields, blob fields
- `logs_schema()` / `LogsBuilder`: Event log fields (address, data, topic0-3)
- `traces_schema()` / `TracesBuilder`: Trace fields (from, to, call_type, input, output, etc.)

Each builder struct has typed Arrow builder fields and a `finish()` method that produces a `RecordBatch`.

#### 4. **svm-decode/** - Solana Instruction & Log Decoding
[svm-decode/Cargo.toml](svm-decode/Cargo.toml) - Decodes Solana instructions and program logs from raw binary into Arrow RecordBatches.

**Key Dependencies:**
- **bs58**, **base64**: Encoding/decoding for Solana-native formats
- **arrow**: Output format
- Optional **pyo3**: Python bindings for `InstructionSignature` and `LogSignature`

**Public API ([svm-decode/src/lib.rs](svm-decode/src/lib.rs)):**
- `decode_instructions_batch()` / `decode_instructions()`: Decodes Solana instruction data given an `InstructionSignature` (discriminator + params + account names)
- `decode_logs_batch()` / `decode_logs()`: Decodes base64-encoded Solana program logs given a `LogSignature`
- `instruction_signature_to_arrow_schema()`: Generates Arrow schema from an instruction signature
- `match_discriminators()`: Validates and strips discriminator bytes from instruction data

**Internal Design:**
- Uses a custom `DynType`/`DynValue` system (in `deserialize` module) supporting Borsh-style types: `U8`-`U256`, `I8`-`I128`, `Bool`, `String`, `Pubkey`, `Array`, `FixedArray`, `Option`, `Struct`, `Enum`, `HashMap`
- Accounts are extracted from columns `a0`-`a9` plus `rest_of_accounts` list column
- Decoded values are converted to Arrow via `arrow_converter` module

#### 5. **svm-schema/** - Solana Schema Definitions
[svm-schema/Cargo.toml](svm-schema/Cargo.toml) - Defines canonical Arrow schemas and builders for Solana data tables.

**Public API ([svm-schema/src/lib.rs](svm-schema/src/lib.rs)):**
- `blocks_schema()` / `BlocksBuilder`: Block fields (slot, hash, parent_slot, height, timestamp)
- `transactions_schema()` / `TransactionsBuilder`: Transaction fields (signature, account_keys, address_table_lookups, fee, etc.)
- `instructions_schema()` / `InstructionsBuilder`: Instruction fields (program_id, accounts a0-a9, rest_of_accounts, data, discriminator prefixes d1/d2/d4/d8)
- `logs_schema()` / `LogsBuilder`: Program log fields (instruction_address, program_id, kind, message)
- `rewards_schema()` / `RewardsBuilder`: Validator reward fields
- `token_balances_schema()` / `TokenBalancesBuilder`: SPL token balance change fields
- `balances_schema()` / `BalancesBuilder`: Native SOL balance change fields

#### 6. **cast/** - Arrow Column Type Casting & Encoding
[cast/Cargo.toml](cast/Cargo.toml) - Provides type casting, encoding/decoding, and U256 conversions for Arrow RecordBatches.

**Key Dependencies:**
- **faster-hex**: High-performance hex encoding
- **bs58**: Base58 encoding (for Solana addresses)
- **alloy-primitives**, **ruint**: U256/I256 arithmetic

**Public API ([cast/src/lib.rs](cast/src/lib.rs)):**
- `cast()` / `cast_schema()`: Cast specific columns by name to target Arrow types
- `cast_by_type()` / `cast_schema_by_type()`: Cast all columns matching a source type to a target type
- `hex_encode()` / `hex_encode_column()`: Convert Binary columns to hex strings (with optional `0x` prefix)
- `base58_encode()` / `base58_encode_column()`: Convert Binary columns to base58 strings
- `hex_decode_column()` / `base58_decode_column()`: Reverse decode operations
- `u256_to_binary()` / `u256_column_to_binary()` / `u256_column_from_binary()`: Convert between Decimal256 and big-endian Binary representations

**Design Note:** Decimal-to-float casts go through an intermediate string representation to allow precision loss gracefully.

#### 7. **query/** - Query Execution Engine
[query/Cargo.toml](query/Cargo.toml) - Filters and joins Arrow RecordBatches using a declarative query structure.

**Key Dependencies:**
- **rayon**: Parallel filter execution across tables and selections
- **hashbrown**: High-performance hash tables for `Contains` filters
- **xxhash-rust**: Fast hashing for hash table lookups
- **arrow** (row module): Row-format conversion for multi-column joins

**Public API ([query/src/lib.rs](query/src/lib.rs)):**
- `run_query()`: Executes a `Query` against a `BTreeMap<TableName, RecordBatch>`, returning filtered tables
- `select_fields()`: Projects tables down to requested field subsets

**Query Model:**
- `Query` contains `selection` (filters + includes per table) and `fields` (columns to return per table)
- `TableSelection` has `filters` (AND'd within a selection) and `include` directives (cross-table joins)
- Multiple selections for the same table are OR'd together
- `Filter` variants: `Contains` (set membership via hash table for 128+ elements, linear scan otherwise), `StartsWith` (prefix matching), `Bool` (boolean column filter)
- `Include` performs semi-joins: filters another table to rows whose join columns match the filtered rows of the source table

#### 8. **ingest/** - Data Ingestion from Providers
[ingest/Cargo.toml](ingest/Cargo.toml) - Streams blockchain data from external providers into Arrow RecordBatches.

**Key Dependencies:**
- **sqd-portal-client**: SQD Network data access
- **hypersync-client**: HyperSync data access
- **tokio**, **futures-lite**: Async streaming
- Optional **pyo3**: Python bindings

**Public API ([ingest/src/lib.rs](ingest/src/lib.rs)):**
- `start_stream()`: Creates an async data stream from a `ProviderConfig` + `Query`
- `ProviderConfig`: Configuration (kind, URL, bearer token, retry settings, buffer size, stop-on-head behavior)
- `ProviderKind`: `Sqd` or `Hypersync`
- `Query`: Either `Evm(evm::Query)` or `Svm(svm::Query)` with chain-specific field selection

**Internal Design:**
- Provider implementations live in [ingest/src/provider/](ingest/src/provider/):
  - `sqd.rs`: SQD Network provider (supports both EVM and SVM)
  - `hypersync.rs`: HyperSync provider (EVM only)
  - `common.rs`: Converts chain-specific queries to generic `cherry_query::Query`
- After raw data is fetched from a provider, `cherry_query::run_query()` is applied locally via Rayon (offloaded from the async runtime using `rayon_async`)
- The stream yields `BTreeMap<String, RecordBatch>` where keys are table names (e.g., "blocks", "transactions", "logs")

#### 9. **python/** - Python Bindings (PyO3)
[python/Cargo.toml](python/Cargo.toml) - Exposes all cherry-core functionality to Python via PyO3, published as the `cherry-core` Python package.

**Public API ([python/src/lib.rs](python/src/lib.rs)):**
All functions accept/return PyArrow objects and delegate to the Rust implementations:
- Cast: `cast()`, `cast_schema()`, `cast_by_type()`, `cast_schema_by_type()`
- Encoding: `hex_encode()`, `prefix_hex_encode()`, `base58_encode()`, and column-level variants
- Decoding: `hex_decode_column()`, `prefix_hex_decode_column()`, `base58_decode_column()`
- U256: `u256_to_binary()`, `u256_column_from_binary()`, `u256_column_to_binary()`
- EVM: `evm_decode_events()`, `evm_decode_call_inputs()`, `evm_decode_call_outputs()`, `evm_event_signature_to_arrow_schema()`, `evm_function_signature_to_arrow_schemas()`, `evm_signature_to_topic0()`
- SVM: `svm_decode_instructions()`, `svm_decode_logs()`, `instruction_signature_to_arrow_schema()`
- Ingest: Submodule at [python/src/ingest.rs](python/src/ingest.rs) exposing `start_stream()`
- Utilities: `base58_encode_bytes()`, `base58_decode_string()`

**Runtime:** Uses jemalloc as the global allocator and a lazy-initialized multi-threaded Tokio runtime for async operations.

## Data Flow

```
User Query (EVM or SVM)
    |
Provider Config (SQD / HyperSync)
    |
+----------------------------------+
|  Ingest (ingest/)                |
|  +- SQD Provider (EVM + SVM)    |
|  +- HyperSync Provider (EVM)    |
+----------------------------------+
    |
Raw Arrow RecordBatches
(BTreeMap<TableName, RecordBatch>)
    |
+----------------------------------+
|  Query Engine (query/)           |
|  +- Filter (Contains/StartsWith)|
|  +- Cross-table Includes        |
|  +- Field Selection             |
+----------------------------------+
    |
Filtered Arrow RecordBatches
    |
+----------------------------------+     +---------------------------+
|  Decode (evm-decode/svm-decode/) |     |  Cast (cast/)             |
|  +- Event/Call decoding (EVM)    |     |  +- Type casting          |
|  +- Instruction/Log decoding     |     |  +- Hex/Base58 encoding   |
|     (SVM)                        |     |  +- U256 conversions      |
+----------------------------------+     +---------------------------+
    |                                        |
    +----------------+-----------------------+
                     |
        Transformed Arrow RecordBatches
                     |
    +-------------------------------+
    |  Python Bindings (python/)    |
    |  (PyArrow <-> Arrow via PyO3) |
    +-------------------------------+
                     |
              cherry (Python ETL)
                     |
    +--------+--------+--------+--------+--------+
    | DuckDB | Iceberg | Delta  | Click- | PyArrow|
    |        |         | Lake   | House  | Dataset|
    +--------+--------+--------+--------+--------+
```

## Key Design Patterns

1. **Arrow-Native**: All data flows as Apache Arrow RecordBatches, enabling zero-copy interop with Python (PyArrow), DataFusion, Polars, and DuckDB
2. **Schema-First**: EVM and SVM schemas are defined as canonical Arrow schemas with matching builder structs, ensuring type consistency across the pipeline
3. **Parallel Execution**: Query filtering uses Rayon for parallel table processing; hash-based `Contains` filters use xxHash for high throughput
4. **Graceful Degradation**: `allow_decode_fail` / `allow_cast_fail` flags throughout let pipelines continue on malformed data by writing nulls instead of erroring
5. **Provider Abstraction**: The ingest layer abstracts over SQD and HyperSync behind a unified streaming interface, with local query post-processing
6. **Async + Rayon Bridge**: CPU-heavy query/decode work is offloaded from the Tokio async runtime to a Rayon thread pool via `rayon_async`
7. **Multi-Chain**: First-class support for both EVM and SVM with chain-specific schemas, decoders, and query types sharing common infrastructure (Arrow, cast, query)

## Licensing

Dual-licensed under MIT or Apache 2.0, at the user's option.
