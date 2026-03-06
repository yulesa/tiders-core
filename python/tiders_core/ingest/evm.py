"""EVM (Ethereum Virtual Machine) query definitions for blockchain data ingestion.

Provides dataclass-based query builders for filtering and selecting EVM blockchain
data including transactions, event logs, and execution traces. Each request type
supports filtering by address, signature hash, and other chain-specific fields.
Field selector classes control which columns are included in the response.
"""

from typing import Optional
from dataclasses import dataclass, field


@dataclass
class TransactionRequest:
    """Filter for EVM transactions.

    All filter fields accept lists of values and are combined with OR logic within
    a field and AND logic across fields. An empty list means no filtering on that field.

    Attributes:
        from_: Sender addresses to match.
        to: Recipient addresses to match.
        sighash: 4-byte function selector hashes to match (hex-encoded).
        status: Transaction status codes (1 for success, 0 for failure).
        type_: Transaction type values (0=legacy, 1=access list, 2=EIP-1559).
        contract_deployment_address: Addresses of deployed contracts to match.
        hash: Specific transaction hashes to match.
        include_logs: If True, include event logs emitted by matching transactions.
        include_traces: If True, include execution traces for matching transactions.
        include_blocks: If True, include block data for blocks containing matches.
    """

    from_: list[str] = field(default_factory=list)
    to: list[str] = field(default_factory=list)
    sighash: list[str] = field(default_factory=list)
    status: list[int] = field(default_factory=list)
    type_: list[int] = field(default_factory=list)
    contract_deployment_address: list[str] = field(default_factory=list)
    hash: list[str] = field(default_factory=list)
    include_logs: bool = False
    include_traces: bool = False
    include_blocks: bool = False


@dataclass
class LogRequest:
    """Filter for EVM event logs.

    Filters are combined with OR logic within a field and AND logic across fields.

    Attributes:
        address: Contract addresses to match.
        topic0: Event signature hashes (keccak256) to match.
        topic1: First indexed parameter values to match.
        topic2: Second indexed parameter values to match.
        topic3: Third indexed parameter values to match.
        include_transactions: If True, include the parent transaction for each log.
        include_transaction_logs: If True, include all logs from matching transactions.
        include_transaction_traces: If True, include traces from matching transactions.
        include_blocks: If True, include block data for blocks containing matches.
    """

    address: list[str] = field(default_factory=list)
    topic0: list[str] = field(default_factory=list)
    topic1: list[str] = field(default_factory=list)
    topic2: list[str] = field(default_factory=list)
    topic3: list[str] = field(default_factory=list)
    include_transactions: bool = False
    include_transaction_logs: bool = False
    include_transaction_traces: bool = False
    include_blocks: bool = False


@dataclass
class TraceRequest:
    """Filter for EVM execution traces (internal transactions).

    Filters are combined with OR logic within a field and AND logic across fields.

    Attributes:
        from_: Caller addresses to match.
        to: Callee addresses to match.
        address: Contract addresses involved in the trace to match.
        call_type: Call types to match (e.g. ``"call"``, ``"delegatecall"``, ``"staticcall"``).
        reward_type: Reward types to match (e.g. ``"block"``, ``"uncle"``).
        type_: Trace types to match (e.g. ``"call"``, ``"create"``, ``"suicide"``).
        sighash: 4-byte function selector hashes to match.
        author: Block reward author addresses to match.
        include_transactions: If True, include the parent transaction.
        include_transaction_logs: If True, include all logs from matching transactions.
        include_transaction_traces: If True, include all traces from matching transactions.
        include_blocks: If True, include block data for blocks containing matches.
    """

    from_: list[str] = field(default_factory=list)
    to: list[str] = field(default_factory=list)
    address: list[str] = field(default_factory=list)
    call_type: list[str] = field(default_factory=list)
    reward_type: list[str] = field(default_factory=list)
    type_: list[str] = field(default_factory=list)
    sighash: list[str] = field(default_factory=list)
    author: list[str] = field(default_factory=list)
    include_transactions: bool = False
    include_transaction_logs: bool = False
    include_transaction_traces: bool = False
    include_blocks: bool = False


@dataclass
class BlockFields:
    """Field selector for EVM block data.

    Set fields to True to include them in the response. All fields default to False.
    """

    number: bool = False
    hash: bool = False
    parent_hash: bool = False
    nonce: bool = False
    sha3_uncles: bool = False
    logs_bloom: bool = False
    transactions_root: bool = False
    state_root: bool = False
    receipts_root: bool = False
    miner: bool = False
    difficulty: bool = False
    total_difficulty: bool = False
    extra_data: bool = False
    size: bool = False
    gas_limit: bool = False
    gas_used: bool = False
    timestamp: bool = False
    uncles: bool = False
    base_fee_per_gas: bool = False
    blob_gas_used: bool = False
    excess_blob_gas: bool = False
    parent_beacon_block_root: bool = False
    withdrawals_root: bool = False
    withdrawals: bool = False
    l1_block_number: bool = False
    send_count: bool = False
    send_root: bool = False
    mix_hash: bool = False


@dataclass
class TransactionFields:
    """Field selector for EVM transaction data.

    Set fields to True to include them in the response. All fields default to False.
    Includes standard EVM fields as well as L2-specific fields (e.g. ``l1_fee``,
    ``blob_gas_price``).
    """

    block_hash: bool = False
    block_number: bool = False
    from_: bool = False
    gas: bool = False
    gas_price: bool = False
    hash: bool = False
    input: bool = False
    nonce: bool = False
    to: bool = False
    transaction_index: bool = False
    value: bool = False
    v: bool = False
    r: bool = False
    s: bool = False
    max_priority_fee_per_gas: bool = False
    max_fee_per_gas: bool = False
    chain_id: bool = False
    cumulative_gas_used: bool = False
    effective_gas_price: bool = False
    gas_used: bool = False
    contract_address: bool = False
    logs_bloom: bool = False
    type_: bool = False
    root: bool = False
    status: bool = False
    sighash: bool = False
    y_parity: bool = False
    access_list: bool = False
    l1_fee: bool = False
    l1_gas_price: bool = False
    l1_fee_scalar: bool = False
    gas_used_for_l1: bool = False
    max_fee_per_blob_gas: bool = False
    blob_versioned_hashes: bool = False
    deposit_nonce: bool = False
    blob_gas_price: bool = False
    deposit_receipt_version: bool = False
    blob_gas_used: bool = False
    l1_base_fee_scalar: bool = False
    l1_blob_base_fee: bool = False
    l1_blob_base_fee_scalar: bool = False
    l1_block_number: bool = False
    mint: bool = False
    source_hash: bool = False


@dataclass
class LogFields:
    """Field selector for EVM event log data.

    Set fields to True to include them in the response. All fields default to False.
    """

    removed: bool = False
    log_index: bool = False
    transaction_index: bool = False
    transaction_hash: bool = False
    block_hash: bool = False
    block_number: bool = False
    address: bool = False
    data: bool = False
    topic0: bool = False
    topic1: bool = False
    topic2: bool = False
    topic3: bool = False


@dataclass
class TraceFields:
    """Field selector for EVM execution trace data.

    Set fields to True to include them in the response. All fields default to False.
    """

    from_: bool = False
    to: bool = False
    call_type: bool = False
    gas: bool = False
    input: bool = False
    init: bool = False
    value: bool = False
    author: bool = False
    reward_type: bool = False
    block_hash: bool = False
    block_number: bool = False
    address: bool = False
    code: bool = False
    gas_used: bool = False
    output: bool = False
    subtraces: bool = False
    trace_address: bool = False
    transaction_hash: bool = False
    transaction_position: bool = False
    type_: bool = False
    error: bool = False
    sighash: bool = False
    action_address: bool = False
    balance: bool = False
    refund_address: bool = False


@dataclass
class Fields:
    """Aggregated field selectors for all EVM data types.

    Attributes:
        block: Fields to include for block data.
        transaction: Fields to include for transaction data.
        log: Fields to include for event log data.
        trace: Fields to include for execution trace data.
    """

    block: BlockFields = field(default_factory=BlockFields)
    transaction: TransactionFields = field(default_factory=TransactionFields)
    log: LogFields = field(default_factory=LogFields)
    trace: TraceFields = field(default_factory=TraceFields)


@dataclass
class Query:
    """Top-level EVM data query.

    Defines the block range, data filters, and field selections for an EVM
    blockchain data request.

    Attributes:
        from_block: Starting block number (inclusive). Defaults to 0.
        to_block: Ending block number (exclusive). If None, streams to chain head.
        include_all_blocks: If True, include all blocks in the range even if they
            don't match any filter.
        transactions: List of transaction filters. Results matching any filter are included.
        logs: List of event log filters. Results matching any filter are included.
        traces: List of trace filters. Results matching any filter are included.
        fields: Field selectors controlling which columns appear in the response.
    """

    from_block: int = 0
    to_block: Optional[int] = None
    include_all_blocks: bool = False
    transactions: list[TransactionRequest] = field(default_factory=list)
    logs: list[LogRequest] = field(default_factory=list)
    traces: list[TraceRequest] = field(default_factory=list)
    fields: Fields = field(default_factory=Fields)
