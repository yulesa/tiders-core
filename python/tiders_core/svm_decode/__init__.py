"""SVM (Solana) instruction and log decoding type system.

Provides a type system for defining Solana instruction and log signatures used
to decode raw binary data into structured Arrow columns. Supports Borsh-serialized
primitive types, arrays, structs, enums, and optionals.

Example::

    from tiders_core.svm_decode import DynType, ParamInput, InstructionSignature

    sig = InstructionSignature(
        discriminator=b"\\xe4\\x45\\xa5...",
        params=[
            ParamInput("amount", DynType.U64),
            ParamInput("data", DynType.Array(DynType.U8)),
        ],
        accounts_names=["source", "destination", "authority"],
    )
"""

from typing import List, Optional, Union, TypeAlias, Literal
from dataclasses import dataclass

PrimitiveType: TypeAlias = Literal[
    "i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "u128", "bool"
]
"""Type alias for Borsh-serializable primitive types."""

ElementType: TypeAlias = Union[
    PrimitiveType, "DynType", "FixedArray", "Array", "Struct", "Enum", "Option"
]
"""Type alias for any element type used in instruction/log signatures."""


@dataclass
class FixedArray:
    """A fixed-size array type.

    Attributes:
        element_type: The type of each element in the array.
        size: The number of elements in the array.
    """

    element_type: ElementType
    size: int


@dataclass
class Array:
    """A variable-length array type (Borsh ``Vec``).

    Attributes:
        element_type: The type of each element in the array.
    """

    element_type: ElementType


@dataclass
class Field:
    """A named field within a struct type.

    Attributes:
        name: The field name.
        element_type: The type of the field value.
    """

    name: str
    element_type: ElementType


@dataclass
class Struct:
    """A struct type composed of named fields.

    Attributes:
        fields: The list of fields that make up the struct.
    """

    fields: List[Field]


@dataclass
class Variant:
    """A variant within an enum type.

    Attributes:
        name: The variant name.
        element_type: The type of data associated with this variant, or None for
            unit variants.
    """

    name: str
    element_type: Optional[ElementType]


@dataclass
class Enum:
    """An enum type with named variants, each optionally carrying data.

    Attributes:
        variants: The list of variants that make up the enum.
    """

    variants: List[Variant]


@dataclass
class Option:
    """An optional type (Borsh ``Option``).

    Attributes:
        element_type: The type of the inner value when present.
    """

    element_type: ElementType


@dataclass
class ParamInput:
    """A named, typed parameter in an instruction or log signature.

    Attributes:
        name: The parameter name, used as the Arrow column name in decoded output.
        param_type: The type of the parameter.
    """

    name: str
    param_type: ElementType


@dataclass
class InstructionSignature:
    """Signature definition for decoding a Solana instruction.

    Defines the discriminator bytes, parameter types, and account names needed
    to decode raw instruction data into structured Arrow columns.

    Attributes:
        discriminator: The instruction discriminator bytes used to identify the
            instruction type. Can be raw bytes or a hex string.
        params: The list of typed parameters to decode from the instruction data
            (after the discriminator).
        accounts_names: Names assigned to positional accounts in the instruction.
            These become column names in the decoded output.
    """

    discriminator: Union[bytes, str]
    params: List[ParamInput]
    accounts_names: List[str]


@dataclass
class LogSignature:
    """Signature definition for decoding a Solana program log event.

    Defines the parameter types needed to decode a base64-encoded program log
    data message into structured Arrow columns.

    Attributes:
        params: The list of typed parameters to decode from the log data.
    """

    params: List[ParamInput]


class DynType:
    """Factory class providing convenient access to all supported types.

    Use class attributes for primitive types and call the nested classes for
    complex types::

        DynType.U64              # unsigned 64-bit integer
        DynType.Array(DynType.U8)  # variable-length byte array
        DynType.Struct([...])    # struct type
        DynType.Option(DynType.Bool)  # optional boolean

    Attributes:
        I8: Signed 8-bit integer.
        I16: Signed 16-bit integer.
        I32: Signed 32-bit integer.
        I64: Signed 64-bit integer.
        I128: Signed 128-bit integer.
        U8: Unsigned 8-bit integer.
        U16: Unsigned 16-bit integer.
        U32: Unsigned 32-bit integer.
        U64: Unsigned 64-bit integer.
        U128: Unsigned 128-bit integer.
        Bool: Boolean type.
        FixedArray: Fixed-size array constructor.
        Array: Variable-length array constructor.
        Struct: Struct type constructor.
        Enum: Enum type constructor.
        Option: Optional type constructor.
    """

    I8: PrimitiveType = "i8"
    I16: PrimitiveType = "i16"
    I32: PrimitiveType = "i32"
    I64: PrimitiveType = "i64"
    I128: PrimitiveType = "i128"
    U8: PrimitiveType = "u8"
    U16: PrimitiveType = "u16"
    U32: PrimitiveType = "u32"
    U64: PrimitiveType = "u64"
    U128: PrimitiveType = "u128"
    Bool: PrimitiveType = "bool"
    FixedArray = FixedArray
    Array = Array
    Struct = Struct
    Enum = Enum
    Option = Option
