"""SVM (Solana Virtual Machine) query definitions for blockchain data ingestion.

Provides dataclass-based query builders for filtering and selecting Solana blockchain
data including instructions, transactions, logs, balances, token balances, and rewards.
Field selector classes control which columns are included in the response.
"""

from typing import Optional, Union
from dataclasses import dataclass, field
from enum import Enum


@dataclass
class InstructionFields:
    """Field selector for Solana instruction data.

    Set fields to True to include them in the response. All fields default to False.

    Account fields ``a0`` through ``a9`` correspond to positional accounts in the
    instruction. ``rest_of_accounts`` captures any accounts beyond index 9.
    Data fields ``d1``, ``d2``, ``d4``, ``d8`` represent data chunks of 1, 2, 4, and
    8 bytes respectively from the instruction data.
    """

    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    instruction_address: bool = False
    program_id: bool = False
    a0: bool = False
    a1: bool = False
    a2: bool = False
    a3: bool = False
    a4: bool = False
    a5: bool = False
    a6: bool = False
    a7: bool = False
    a8: bool = False
    a9: bool = False
    rest_of_accounts: bool = False
    data: bool = False
    d1: bool = False
    d2: bool = False
    d4: bool = False
    d8: bool = False
    error: bool = False
    compute_units_consumed: bool = False
    is_committed: bool = False
    has_dropped_log_messages: bool = False


@dataclass
class TransactionFields:
    """Field selector for Solana transaction data.

    Set fields to True to include them in the response. All fields default to False.
    """

    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    signature: bool = False
    version: bool = False
    account_keys: bool = False
    address_table_lookups: bool = False
    num_readonly_signed_accounts: bool = False
    num_readonly_unsigned_accounts: bool = False
    num_required_signatures: bool = False
    recent_blockhash: bool = False
    signatures: bool = False
    err: bool = False
    fee: bool = False
    compute_units_consumed: bool = False
    loaded_readonly_addresses: bool = False
    loaded_writable_addresses: bool = False
    fee_payer: bool = False
    has_dropped_log_messages: bool = False


@dataclass
class LogFields:
    """Field selector for Solana program log data.

    Set fields to True to include them in the response. All fields default to False.
    """

    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    log_index: bool = False
    instruction_address: bool = False
    program_id: bool = False
    kind: bool = False
    message: bool = False


@dataclass
class BalanceFields:
    """Field selector for Solana native SOL balance change data.

    Set fields to True to include them in the response. All fields default to False.

    Attributes:
        block_slot: The slot number of the block.
        block_hash: The hash of the block.
        transaction_index: The index of the transaction within the block.
        account: The account public key.
        pre: The account balance before the transaction (in lamports).
        post: The account balance after the transaction (in lamports).
    """

    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    account: bool = False
    pre: bool = False
    post: bool = False


@dataclass
class TokenBalanceFields:
    """Field selector for Solana SPL token balance change data.

    Set fields to True to include them in the response. All fields default to False.
    Pre/post fields capture the state before and after the transaction.
    """

    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    account: bool = False
    pre_mint: bool = False
    post_mint: bool = False
    pre_decimals: bool = False
    post_decimals: bool = False
    pre_program_id: bool = False
    post_program_id: bool = False
    pre_owner: bool = False
    post_owner: bool = False
    pre_amount: bool = False
    post_amount: bool = False


@dataclass
class RewardFields:
    """Field selector for Solana validator reward data.

    Set fields to True to include them in the response. All fields default to False.
    """

    block_slot: bool = False
    block_hash: bool = False
    pubkey: bool = False
    lamports: bool = False
    post_balance: bool = False
    reward_type: bool = False
    commission: bool = False


@dataclass
class BlockFields:
    """Field selector for Solana block data.

    Set fields to True to include them in the response. All fields default to False.
    """

    slot: bool = False
    hash: bool = False
    parent_slot: bool = False
    parent_hash: bool = False
    height: bool = False
    timestamp: bool = False


@dataclass
class Fields:
    """Aggregated field selectors for all SVM data types.

    Attributes:
        instruction: Fields to include for instruction data.
        transaction: Fields to include for transaction data.
        log: Fields to include for program log data.
        balance: Fields to include for SOL balance change data.
        token_balance: Fields to include for SPL token balance change data.
        reward: Fields to include for validator reward data.
        block: Fields to include for block data.
    """

    instruction: InstructionFields = field(default_factory=InstructionFields)
    transaction: TransactionFields = field(default_factory=TransactionFields)
    log: LogFields = field(default_factory=LogFields)
    balance: BalanceFields = field(default_factory=BalanceFields)
    token_balance: TokenBalanceFields = field(default_factory=TokenBalanceFields)
    reward: RewardFields = field(default_factory=RewardFields)
    block: BlockFields = field(default_factory=BlockFields)


@dataclass
class InstructionRequest:
    """Filter for Solana instructions.

    All filter fields accept lists of values and are combined with OR logic within
    a field and AND logic across fields. An empty list means no filtering on that field.

    Discriminator fields (``d1``-``d8``) filter on instruction data prefixes of the
    corresponding byte length. Account fields (``a0``-``a9``) filter on positional
    account public keys.

    Attributes:
        program_id: Program IDs to match (base58-encoded).
        discriminator: Instruction discriminators to match (bytes or hex strings).
        d1: 1-byte data prefix filters.
        d2: 2-byte data prefix filters.
        d3: 3-byte data prefix filters.
        d4: 4-byte data prefix filters.
        d8: 8-byte data prefix filters.
        a0: Account at index 0 to match (base58-encoded).
        a1: Account at index 1 to match.
        a2: Account at index 2 to match.
        a3: Account at index 3 to match.
        a4: Account at index 4 to match.
        a5: Account at index 5 to match.
        a6: Account at index 6 to match.
        a7: Account at index 7 to match.
        a8: Account at index 8 to match.
        a9: Account at index 9 to match.
        is_committed: If True, only include committed (successful) instructions.
        include_transactions: If True, include the parent transaction.
        include_transaction_token_balances: If True, include token balance changes.
        include_logs: If True, include program logs for matching instructions.
        include_inner_instructions: If True, include inner (CPI) instructions.
        include_blocks: If True, include block data. Defaults to True.
    """

    program_id: list[str] = field(default_factory=list)
    discriminator: list[Union[bytes, str]] = field(default_factory=list)
    d1: list[Union[bytes, str]] = field(default_factory=list)
    d2: list[Union[bytes, str]] = field(default_factory=list)
    d3: list[Union[bytes, str]] = field(default_factory=list)
    d4: list[Union[bytes, str]] = field(default_factory=list)
    d8: list[Union[bytes, str]] = field(default_factory=list)
    a0: list[str] = field(default_factory=list)
    a1: list[str] = field(default_factory=list)
    a2: list[str] = field(default_factory=list)
    a3: list[str] = field(default_factory=list)
    a4: list[str] = field(default_factory=list)
    a5: list[str] = field(default_factory=list)
    a6: list[str] = field(default_factory=list)
    a7: list[str] = field(default_factory=list)
    a8: list[str] = field(default_factory=list)
    a9: list[str] = field(default_factory=list)
    is_committed: bool = False
    include_transactions: bool = False
    include_transaction_token_balances: bool = False
    include_logs: bool = False
    include_inner_instructions: bool = False
    include_blocks: bool = True


@dataclass
class TransactionRequest:
    """Filter for Solana transactions.

    Attributes:
        fee_payer: Fee payer public keys to match (base58-encoded).
        include_instructions: If True, include all instructions in matching transactions.
        include_logs: If True, include program logs for matching transactions.
        include_blocks: If True, include block data for blocks containing matches.
    """

    fee_payer: list[str] = field(default_factory=list)
    include_instructions: bool = False
    include_logs: bool = False
    include_blocks: bool = False


class LogKind(str, Enum):
    """Solana program log message types.

    Attributes:
        LOG: Standard program log messages (``sol_log``).
        DATA: Base64-encoded program data messages (``sol_log_data``).
        OTHER: Other log message types.
    """

    LOG = "log"
    DATA = "data"
    OTHER = "other"


@dataclass
class LogRequest:
    """Filter for Solana program logs.

    Attributes:
        program_id: Program IDs to match (base58-encoded).
        kind: Log message types to match.
        include_transactions: If True, include the parent transaction.
        include_instructions: If True, include the instruction that emitted the log.
        include_blocks: If True, include block data for blocks containing matches.
    """

    program_id: list[str] = field(default_factory=list)
    kind: list[LogKind] = field(default_factory=list)
    include_transactions: bool = False
    include_instructions: bool = False
    include_blocks: bool = False


@dataclass
class BalanceRequest:
    """Filter for Solana native SOL balance changes.

    Attributes:
        account: Account public keys to match (base58-encoded).
        include_transactions: If True, include the parent transaction.
        include_transaction_instructions: If True, include instructions from matching
            transactions.
        include_blocks: If True, include block data for blocks containing matches.
    """

    account: list[str] = field(default_factory=list)
    include_transactions: bool = False
    include_transaction_instructions: bool = False
    include_blocks: bool = False


@dataclass
class TokenBalanceRequest:
    """Filter for Solana SPL token balance changes.

    Pre/post filters match the state before and after the transaction respectively.

    Attributes:
        account: Token account public keys to match (base58-encoded).
        pre_program_id: Token program IDs before the transaction.
        post_program_id: Token program IDs after the transaction.
        pre_mint: Token mint addresses before the transaction.
        post_mint: Token mint addresses after the transaction.
        pre_owner: Token account owners before the transaction.
        post_owner: Token account owners after the transaction.
        include_transactions: If True, include the parent transaction.
        include_transaction_instructions: If True, include instructions from matching
            transactions.
        include_blocks: If True, include block data for blocks containing matches.
    """

    account: list[str] = field(default_factory=list)
    pre_program_id: list[str] = field(default_factory=list)
    post_program_id: list[str] = field(default_factory=list)
    pre_mint: list[str] = field(default_factory=list)
    post_mint: list[str] = field(default_factory=list)
    pre_owner: list[str] = field(default_factory=list)
    post_owner: list[str] = field(default_factory=list)
    include_transactions: bool = False
    include_transaction_instructions: bool = False
    include_blocks: bool = False


@dataclass
class RewardRequest:
    """Filter for Solana validator rewards.

    Attributes:
        pubkey: Validator public keys to match (base58-encoded).
        include_blocks: If True, include block data for blocks containing matches.
    """

    pubkey: list[str] = field(default_factory=list)
    include_blocks: bool = False


@dataclass
class Query:
    """Top-level SVM data query.

    Defines the block range (by slot number), data filters, and field selections
    for a Solana blockchain data request.

    Attributes:
        from_block: Starting slot number (inclusive). Defaults to 0.
        to_block: Ending slot number (exclusive). If None, streams to chain head.
        include_all_blocks: If True, include all blocks in the range even if they
            don't match any filter.
        fields: Field selectors controlling which columns appear in the response.
        instructions: List of instruction filters. Results matching any filter are included.
        transactions: List of transaction filters.
        logs: List of program log filters.
        balances: List of SOL balance change filters.
        token_balances: List of SPL token balance change filters.
        rewards: List of validator reward filters.
    """

    from_block: int = 0
    to_block: Optional[int] = None
    include_all_blocks: bool = False
    fields: Fields = field(default_factory=Fields)
    instructions: list[InstructionRequest] = field(default_factory=list)
    transactions: list[TransactionRequest] = field(default_factory=list)
    logs: list[LogRequest] = field(default_factory=list)
    balances: list[BalanceRequest] = field(default_factory=list)
    token_balances: list[TokenBalanceRequest] = field(default_factory=list)
    rewards: list[RewardRequest] = field(default_factory=list)
