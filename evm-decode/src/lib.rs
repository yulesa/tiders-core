use std::sync::Arc;

use alloy_dyn_abi::{DynSolCall, DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy_json_abi::JsonAbi;
use alloy_primitives::{I256, U256};
use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{
        builder, Array, ArrowPrimitiveType, BinaryArray, BooleanArray, GenericBinaryArray,
        LargeBinaryArray, ListArray, OffsetSizeTrait, RecordBatch, StructArray,
    },
    buffer::{NullBuffer, OffsetBuffer},
    compute,
    datatypes::{
        DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Int8Type, Schema, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

/// Returns topic0 based on given event signature
pub fn signature_to_topic0(signature: &str) -> Result<[u8; 32]> {
    let event = alloy_json_abi::Event::parse(signature).context("parse event signature")?;
    Ok(event.selector().into())
}

/// Decodes given call input data in arrow format to arrow format.
/// Output Arrow schema is auto generated based on the function signature.
/// Handles any level of nesting with Lists/Structs.
///
/// Writes `null` for data rows that fail to decode if `allow_decode_fail` is set to `true`.
/// Errors when a row fails to decode if `allow_decode_fail` is set to `false`.
pub fn decode_call_inputs<I: OffsetSizeTrait>(
    signature: &str,
    data: &GenericBinaryArray<I>,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    decode_call_impl::<true, I>(signature, data, allow_decode_fail)
}

/// Decodes given call output data in arrow format to arrow format.
/// Output Arrow schema is auto generated based on the function signature.
/// Handles any level of nesting with Lists/Structs.
///
/// Writes `null` for data rows that fail to decode if `allow_decode_fail` is set to `true`.
/// Errors when a row fails to decode if `allow_decode_fail` is set to `false`.
pub fn decode_call_outputs<I: OffsetSizeTrait>(
    signature: &str,
    data: &GenericBinaryArray<I>,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    decode_call_impl::<false, I>(signature, data, allow_decode_fail)
}

// IS_INPUT: true means we are decoding inputs
// false means we are decoding outputs
fn decode_call_impl<const IS_INPUT: bool, I: OffsetSizeTrait>(
    signature: &str,
    data: &GenericBinaryArray<I>,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let (call, resolved) = resolve_function_signature(signature)?;

    let schema = function_signature_to_arrow_schemas_impl(&call, &resolved)
        .context("convert event signature to arrow schema")?;
    let schema = if IS_INPUT { schema.0 } else { schema.1 };

    let mut arrays: Vec<Arc<dyn Array + 'static>> = Vec::with_capacity(schema.fields().len());

    let mut decoded = Vec::<Option<DynSolValue>>::with_capacity(data.len());

    for blob in data {
        match blob {
            Some(blob) => {
                let decode_res = if IS_INPUT {
                    resolved.abi_decode_input(blob)
                } else {
                    resolved.abi_decode_output(blob)
                };
                match decode_res {
                    Ok(data) => decoded.push(Some(DynSolValue::Tuple(data))),
                    Err(e) if allow_decode_fail => {
                        log::debug!("failed to decode function call data: {e}");
                        decoded.push(None);
                    }
                    Err(e) => {
                        return Err(anyhow!("failed to decode function call data: {e}"));
                    }
                }
            }
            None => decoded.push(None),
        }
    }

    let sol_type = if IS_INPUT {
        DynSolType::Tuple(resolved.types().to_vec())
    } else {
        DynSolType::Tuple(resolved.returns().types().to_vec())
    };

    let array = to_arrow(&sol_type, decoded, allow_decode_fail).context("map params to arrow")?;
    let arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .context("expected struct array from to_arrow")?;

    for f in arr.columns() {
        arrays.push(f.clone());
    }

    RecordBatch::try_new(Arc::new(schema), arrays).context("construct arrow batch")
}

/// Returns (input schema, output schema)
pub fn function_signature_to_arrow_schemas(signature: &str) -> Result<(Schema, Schema)> {
    let (func, resolved) = resolve_function_signature(signature)?;
    function_signature_to_arrow_schemas_impl(&func, &resolved)
}

fn function_signature_to_arrow_schemas_impl(
    func: &alloy_json_abi::Function,
    call: &DynSolCall,
) -> Result<(Schema, Schema)> {
    let mut input_fields = Vec::with_capacity(call.types().len());
    let mut output_fields = Vec::with_capacity(call.returns().types().len());

    for (i, (sol_t, param)) in call.types().iter().zip(func.inputs.iter()).enumerate() {
        let dtype = to_arrow_dtype(sol_t).context("map to arrow type")?;
        let name = if param.name() == "" {
            format!("param{i}")
        } else {
            param.name().to_owned()
        };
        input_fields.push(Arc::new(Field::new(name, dtype, true)));
    }

    for (i, (sol_t, param)) in call
        .returns()
        .types()
        .iter()
        .zip(func.outputs.iter())
        .enumerate()
    {
        let dtype = to_arrow_dtype(sol_t).context("map to arrow type")?;
        let name = if param.name() == "" {
            format!("param{i}")
        } else {
            param.name().to_owned()
        };
        output_fields.push(Arc::new(Field::new(name, dtype, true)));
    }

    Ok((Schema::new(input_fields), Schema::new(output_fields)))
}

fn resolve_function_signature(signature: &str) -> Result<(alloy_json_abi::Function, DynSolCall)> {
    let event = alloy_json_abi::Function::parse(signature).context("parse function signature")?;
    let resolved = event.resolve().context("resolve function signature")?;

    Ok((event, resolved))
}

/// Decodes given event data in arrow format to arrow format.
/// Output Arrow schema is auto generated based on the event signature.
/// Handles any level of nesting with Lists/Structs.
///
/// When `filter_by_topic0` is `true`, only rows whose `topic0` column matches the
/// event's selector are decoded. Non-matching rows are silently filtered out.
///
/// When `hstack` is `true`, the original input columns (after any topic0 filtering)
/// are appended alongside the decoded columns in the output.
///
/// Writes `null` for data rows that fail to decode if `allow_decode_fail` is set to `true`.
/// Errors when a row fails to decode if `allow_decode_fail` is set to `false`.
pub fn decode_events(
    signature: &str,
    data: &RecordBatch,
    allow_decode_fail: bool,
    filter_by_topic0: bool,
    hstack: bool,
) -> Result<RecordBatch> {
    let (event, resolved) = resolve_event_signature(signature)?;

    // Optionally filter input to only rows whose topic0 matches the event selector.
    let data = if filter_by_topic0 {
        filter_by_topic0_impl(&event, data)?
    } else {
        data.clone()
    };

    let schema = event_signature_to_arrow_schema_impl(&event, &resolved)
        .context("convert event signature to arrow schema")?;

    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut arrays: Vec<Arc<dyn Array + 'static>> = Vec::with_capacity(fields.len());

    for (sol_type, topic_name) in resolved
        .indexed()
        .iter()
        .zip(&["topic1", "topic2", "topic3"])
    {
        let col = data
            .column_by_name(topic_name)
            .context("get topic column")?;

        if col.data_type() == &DataType::Binary {
            let arr = col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("downcast to BinaryArray")?;
            decode_topic(sol_type, arr, allow_decode_fail, &mut arrays).context("decode topic")?;
        } else if col.data_type() == &DataType::LargeBinary {
            let arr = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .context("downcast to LargeBinaryArray")?;
            decode_topic(sol_type, arr, allow_decode_fail, &mut arrays).context("decode topic")?;
        }
    }

    let body_col = data.column_by_name("data").context("get data column")?;

    let body_sol_type = DynSolType::Tuple(resolved.body().to_vec());

    if body_col.data_type() == &DataType::Binary {
        let arr = body_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("downcast to BinaryArray")?;
        decode_body(&body_sol_type, arr, allow_decode_fail, &mut arrays).context("decode body")?;
    } else if body_col.data_type() == &DataType::LargeBinary {
        let arr = body_col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .context("downcast to LargeBinaryArray")?;
        decode_body(&body_sol_type, arr, allow_decode_fail, &mut arrays).context("decode body")?;
    }

    if hstack {
        for (i, col) in data.columns().iter().enumerate() {
            fields.push(data.schema().field(i).clone().into());
            arrays.push(col.clone());
        }
    }

    let output_schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(output_schema), arrays).context("construct arrow batch")
}

fn decode_body<I: OffsetSizeTrait>(
    body_sol_type: &DynSolType,
    body_col: &GenericBinaryArray<I>,
    allow_decode_fail: bool,
    arrays: &mut Vec<Arc<dyn Array>>,
) -> Result<()> {
    let mut body_decoded = Vec::<Option<DynSolValue>>::with_capacity(body_col.len());

    for blob in body_col {
        match blob {
            Some(blob) => match body_sol_type.abi_decode_sequence(blob) {
                Ok(data) => body_decoded.push(Some(data)),
                Err(e) if allow_decode_fail => {
                    log::debug!("failed to decode body: {e}");
                    body_decoded.push(None);
                }
                Err(e) => {
                    return Err(anyhow!("failed to decode body: {e}"));
                }
            },
            None => body_decoded.push(None),
        }
    }

    let body_array =
        to_arrow(body_sol_type, body_decoded, allow_decode_fail).context("map body to arrow")?;
    let arr = body_array
        .as_any()
        .downcast_ref::<StructArray>()
        .context("expected struct array from to_arrow")?;

    for f in arr.columns() {
        arrays.push(f.clone());
    }

    Ok(())
}

fn decode_topic<I: OffsetSizeTrait>(
    sol_type: &DynSolType,
    col: &GenericBinaryArray<I>,
    allow_decode_fail: bool,
    arrays: &mut Vec<Arc<dyn Array>>,
) -> Result<()> {
    let mut decoded = Vec::<Option<DynSolValue>>::with_capacity(col.len());

    for blob in col {
        match blob {
            Some(blob) => match sol_type.abi_decode(blob) {
                Ok(data) => decoded.push(Some(data)),
                Err(e) if allow_decode_fail => {
                    log::debug!("failed to decode a topic: {e}");
                    decoded.push(None);
                }
                Err(e) => {
                    return Err(anyhow!("failed to decode a topic: {e}"));
                }
            },
            None => decoded.push(None),
        }
    }

    arrays.push(to_arrow(sol_type, decoded, allow_decode_fail).context("map topic to arrow")?);

    Ok(())
}

/// Generates Arrow schema based on given event signature
pub fn event_signature_to_arrow_schema(signature: &str) -> Result<Schema> {
    let (resolved, event) = resolve_event_signature(signature)?;
    event_signature_to_arrow_schema_impl(&resolved, &event)
}

fn event_signature_to_arrow_schema_impl(
    sig: &alloy_json_abi::Event,
    event: &DynSolEvent,
) -> Result<Schema> {
    let num_fields = event.indexed().len() + event.body().len();
    let mut fields = Vec::<Arc<Field>>::with_capacity(num_fields);
    let mut names = Vec::with_capacity(num_fields);

    for (i, input) in sig.inputs.iter().enumerate() {
        if input.indexed {
            let name = if input.name.is_empty() {
                format!("param{i}")
            } else {
                input.name.clone()
            };
            names.push(name);
        }
    }
    for (i, input) in sig.inputs.iter().enumerate() {
        if !input.indexed {
            let name = if input.name.is_empty() {
                format!("param{i}")
            } else {
                input.name.clone()
            };
            names.push(name);
        }
    }

    for (sol_t, name) in event.indexed().iter().chain(event.body()).zip(names) {
        let dtype = to_arrow_dtype(sol_t).context("map to arrow type")?;
        fields.push(Arc::new(Field::new(name, dtype, true)));
    }

    Ok(Schema::new(fields))
}

fn resolve_event_signature(signature: &str) -> Result<(alloy_json_abi::Event, DynSolEvent)> {
    let event = alloy_json_abi::Event::parse(signature).context("parse event signature")?;
    let resolved = event.resolve().context("resolve event signature")?;

    Ok((event, resolved))
}

/// Filters a RecordBatch to only rows where the `topic0` column matches the
/// event's selector hash. If `topic0` column is not present, returns the data
/// unchanged. Non-matching rows are always silently filtered out.
fn filter_by_topic0_impl(event: &alloy_json_abi::Event, data: &RecordBatch) -> Result<RecordBatch> {
    let Some(topic0_col) = data.column_by_name("topic0") else {
        return Ok(data.clone());
    };

    let selector = event.selector();

    let mask =
        build_topic0_mask(topic0_col, selector.as_slice()).context("build topic0 filter mask")?;

    let non_matching = mask.iter().filter(|v| !v.unwrap_or(false)).count();
    if non_matching > 0 {
        log::debug!(
            "filtering out {non_matching} events whose topic0 does not match '{}'",
            event.full_signature()
        );
    }

    compute::filter_record_batch(data, &mask).context("filter record batch by topic0")
}

fn build_topic0_mask(col: &dyn Array, selector: &[u8]) -> Result<BooleanArray> {
    if col.data_type() == &DataType::Binary {
        let arr = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("downcast topic0 to BinaryArray")?;
        Ok(arr
            .iter()
            .map(|v| v.map(|b| b == selector))
            .collect::<BooleanArray>())
    } else if col.data_type() == &DataType::LargeBinary {
        let arr = col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .context("downcast topic0 to LargeBinaryArray")?;
        Ok(arr
            .iter()
            .map(|v| v.map(|b| b == selector))
            .collect::<BooleanArray>())
    } else {
        Err(anyhow!(
            "unexpected data type for topic0 column: {:?}",
            col.data_type()
        ))
    }
}

fn to_arrow_dtype(sol_type: &DynSolType) -> Result<DataType> {
    match sol_type {
        DynSolType::Bool => Ok(DataType::Boolean),
        DynSolType::Bytes | DynSolType::Address | DynSolType::FixedBytes(_) => Ok(DataType::Binary),
        DynSolType::String => Ok(DataType::Utf8),
        DynSolType::Int(num_bits) => Ok(num_bits_to_int_type(*num_bits)),
        DynSolType::Uint(num_bits) => Ok(num_bits_to_uint_type(*num_bits)),
        DynSolType::Array(inner_type) | DynSolType::FixedArray(inner_type, _) => {
            let inner_type = to_arrow_dtype(inner_type).context("map inner")?;
            Ok(DataType::List(Arc::new(Field::new("", inner_type, true))))
        }
        DynSolType::Function => Err(anyhow!(
            "decoding 'Function' typed value in function signature isn't supported."
        )),
        DynSolType::Tuple(fields) => {
            let mut arrow_fields = Vec::<Arc<Field>>::with_capacity(fields.len());

            for (i, f) in fields.iter().enumerate() {
                let inner_dt = to_arrow_dtype(f).context("map field dt")?;
                arrow_fields.push(Arc::new(Field::new(format!("param{i}"), inner_dt, true)));
            }

            Ok(DataType::Struct(Fields::from(arrow_fields)))
        }
    }
}

fn num_bits_to_uint_type(num_bits: usize) -> DataType {
    if num_bits <= 8 {
        DataType::UInt8
    } else if num_bits <= 16 {
        DataType::UInt16
    } else if num_bits <= 32 {
        DataType::UInt32
    } else if num_bits <= 64 {
        DataType::UInt64
    } else if num_bits <= 128 {
        DataType::Decimal128(38, 0)
    } else if num_bits <= 256 {
        DataType::Decimal256(76, 0)
    } else {
        unreachable!()
    }
}

fn num_bits_to_int_type(num_bits: usize) -> DataType {
    if num_bits <= 8 {
        DataType::Int8
    } else if num_bits <= 16 {
        DataType::Int16
    } else if num_bits <= 32 {
        DataType::Int32
    } else if num_bits <= 64 {
        DataType::Int64
    } else if num_bits <= 128 {
        DataType::Decimal128(38, 0)
    } else if num_bits <= 256 {
        DataType::Decimal256(76, 0)
    } else {
        unreachable!()
    }
}

fn to_arrow(
    sol_type: &DynSolType,
    sol_values: Vec<Option<DynSolValue>>,
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    match sol_type {
        DynSolType::Bool => to_bool(&sol_values),
        DynSolType::Bytes | DynSolType::Address | DynSolType::FixedBytes(_) => {
            to_binary(&sol_values)
        }
        DynSolType::String => to_string(&sol_values),
        DynSolType::Int(num_bits) => to_int(*num_bits, &sol_values, allow_decode_fail),
        DynSolType::Uint(num_bits) => to_uint(*num_bits, &sol_values, allow_decode_fail),
        DynSolType::Array(inner_type) | DynSolType::FixedArray(inner_type, _) => {
            to_list(inner_type, sol_values, allow_decode_fail)
        }
        DynSolType::Function => Err(anyhow!(
            "decoding 'Function' typed value in function signature isn't supported."
        )),
        DynSolType::Tuple(fields) => to_struct(fields, sol_values, allow_decode_fail),
    }
}

fn to_int(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    match num_bits_to_int_type(num_bits) {
        DataType::Int8 => to_int_impl::<Int8Type>(num_bits, sol_values, allow_decode_fail),
        DataType::Int16 => to_int_impl::<Int16Type>(num_bits, sol_values, allow_decode_fail),
        DataType::Int32 => to_int_impl::<Int32Type>(num_bits, sol_values, allow_decode_fail),
        DataType::Int64 => to_int_impl::<Int64Type>(num_bits, sol_values, allow_decode_fail),
        DataType::Decimal128(_, _) => to_decimal128(num_bits, sol_values, allow_decode_fail),
        DataType::Decimal256(_, _) => to_decimal256(num_bits, sol_values, allow_decode_fail),
        dt => Err(anyhow!("unexpected int data type: {dt:?}")),
    }
}

fn to_uint(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    match num_bits_to_uint_type(num_bits) {
        DataType::UInt8 => to_int_impl::<UInt8Type>(num_bits, sol_values, allow_decode_fail),
        DataType::UInt16 => to_int_impl::<UInt16Type>(num_bits, sol_values, allow_decode_fail),
        DataType::UInt32 => to_int_impl::<UInt32Type>(num_bits, sol_values, allow_decode_fail),
        DataType::UInt64 => to_int_impl::<UInt64Type>(num_bits, sol_values, allow_decode_fail),
        DataType::Decimal128(_, _) => to_decimal128(num_bits, sol_values, allow_decode_fail),
        DataType::Decimal256(_, _) => to_decimal256(num_bits, sol_values, allow_decode_fail),
        dt => Err(anyhow!("unexpected uint data type: {dt:?}")),
    }
}

fn to_decimal128(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut builder = builder::Decimal128Builder::new();

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Int(v, nb) => {
                    if num_bits != *nb {
                        return Err(anyhow!("bit width mismatch: expected {num_bits}, got {nb}"));
                    }

                    match i128::try_from(*v) {
                        Ok(v) => builder.append_value(v),
                        Err(e) if allow_decode_fail => {
                            log::debug!("failed to convert int value to i128: {e}");
                            builder.append_null();
                        }
                        Err(e) => {
                            return Err(anyhow!("convert to i128: {e}"));
                        }
                    }
                }
                DynSolValue::Uint(v, nb) => {
                    if num_bits != *nb {
                        return Err(anyhow!("bit width mismatch: expected {num_bits}, got {nb}"));
                    }

                    match i128::try_from(*v) {
                        Ok(v) => builder.append_value(v),
                        Err(e) if allow_decode_fail => {
                            log::debug!("failed to convert uint value to i128: {e}");
                            builder.append_null();
                        }
                        Err(e) => {
                            return Err(anyhow!("convert to i128: {e}"));
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: int/uint, Found: {val:?}"
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    builder = builder.with_data_type(DataType::Decimal128(38, 0));

    Ok(Arc::new(builder.finish()))
}

fn to_decimal256(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut builder = builder::Decimal256Builder::new();

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Int(v, nb) => {
                    if num_bits != *nb {
                        return Err(anyhow!("bit width mismatch: expected {num_bits}, got {nb}"));
                    }

                    let v = arrow::datatypes::i256::from_be_bytes(v.to_be_bytes::<32>());

                    builder.append_value(v);
                }
                DynSolValue::Uint(v, nb) => {
                    if num_bits != *nb {
                        return Err(anyhow!("bit width mismatch: expected {num_bits}, got {nb}"));
                    }
                    match I256::try_from(*v).context("try u256 to i256") {
                        Ok(v) => builder.append_value(arrow::datatypes::i256::from_be_bytes(
                            v.to_be_bytes::<32>(),
                        )),
                        Err(e) => {
                            if allow_decode_fail {
                                log::debug!("failed to decode u256: {e}");
                                builder.append_null();
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: bool, Found: {val:?}"
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    builder = builder.with_data_type(DataType::Decimal256(76, 0));

    Ok(Arc::new(builder.finish()))
}

fn to_int_impl<T>(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<I256> + TryFrom<U256>,
{
    let mut builder = builder::PrimitiveBuilder::<T>::new();

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Int(v, nb) => {
                    if num_bits != *nb {
                        return Err(anyhow!("bit width mismatch: expected {num_bits}, got {nb}"));
                    }
                    match T::Native::try_from(*v) {
                        Ok(native) => builder.append_value(native),
                        Err(_) if allow_decode_fail => {
                            log::debug!("failed to convert int value {v} to native type");
                            builder.append_null();
                        }
                        Err(_) => {
                            return Err(anyhow!("failed to convert int value to native type"));
                        }
                    }
                }
                DynSolValue::Uint(v, nb) => {
                    if num_bits != *nb {
                        return Err(anyhow!("bit width mismatch: expected {num_bits}, got {nb}"));
                    }
                    match T::Native::try_from(*v) {
                        Ok(native) => builder.append_value(native),
                        Err(_) if allow_decode_fail => {
                            log::debug!("failed to convert uint value {v} to native type");
                            builder.append_null();
                        }
                        Err(_) => {
                            return Err(anyhow!("failed to convert uint value to native type"));
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: int/uint, Found: {val:?}"
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn to_list(
    sol_type: &DynSolType,
    sol_values: Vec<Option<DynSolValue>>,
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut lengths = Vec::with_capacity(sol_values.len());
    let mut values = Vec::with_capacity(sol_values.len() * 2);
    let mut validity = Vec::with_capacity(sol_values.len() * 2);

    let mut all_valid = true;

    for val in sol_values {
        if let Some(val) = val {
            match val {
                DynSolValue::Array(inner_vals) | DynSolValue::FixedArray(inner_vals) => {
                    lengths.push(inner_vals.len());
                    values.extend(inner_vals.into_iter().map(Some));
                    validity.push(true);
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected list type, Found: {val:?}"
                    ));
                }
            }
        } else {
            lengths.push(0);
            validity.push(false);
            all_valid = false;
        }
    }

    let values = to_arrow(sol_type, values, allow_decode_fail).context("map inner")?;
    let field = Field::new(
        "",
        to_arrow_dtype(sol_type).context("construct data type")?,
        true,
    );
    let list_arr = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::from_lengths(lengths),
        values,
        if all_valid {
            None
        } else {
            Some(NullBuffer::from(validity))
        },
    )
    .context("construct list array")?;
    Ok(Arc::new(list_arr))
}

fn to_struct(
    fields: &[DynSolType],
    sol_values: Vec<Option<DynSolValue>>,
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut values = vec![Vec::with_capacity(sol_values.len()); fields.len()];

    // unpack top layer of sol_values into columnar format
    // since we recurse by calling to_arrow later in the function, this will eventually map to
    // primitive types.
    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Tuple(inner_vals) => {
                    if values.len() != inner_vals.len() {
                        let expected = values.len();
                        let found = inner_vals.len();
                        return Err(anyhow!(
                            "found unexpected length tuple value. Expected: {expected}, Found: {found}"
                        ));
                    }
                    for (v, inner) in values.iter_mut().zip(inner_vals) {
                        v.push(Some(inner.clone()));
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: tuple, Found: {val:?}"
                    ));
                }
            },
            None => {
                for v in &mut values {
                    v.push(None);
                }
            }
        }
    }

    let mut arrays = Vec::with_capacity(fields.len());

    for (sol_type, arr_vals) in fields.iter().zip(values.into_iter()) {
        arrays.push(to_arrow(sol_type, arr_vals, allow_decode_fail)?);
    }

    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, arr)| Field::new(format!("param{i}"), arr.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays).context("construct record batch")?;

    Ok(Arc::new(StructArray::from(batch)))
}

fn to_bool(sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BooleanBuilder::new();

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Bool(b) => {
                    builder.append_value(*b);
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: bool, Found: {val:?}"
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn to_binary(sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BinaryBuilder::new();

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Bytes(data) => {
                    builder.append_value(data);
                }
                DynSolValue::FixedBytes(data, _) => {
                    builder.append_value(data);
                }
                DynSolValue::Address(data) => {
                    builder.append_value(data);
                }
                DynSolValue::Uint(v, _) => {
                    builder.append_value(v.to_be_bytes::<32>());
                }
                DynSolValue::Int(v, _) => {
                    builder.append_value(v.to_be_bytes::<32>());
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected a binary type, Found: {val:?}"
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn to_string(sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::StringBuilder::new();

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::String(s) => {
                    builder.append_value(s);
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected string, Found: {val:?}"
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parsed event info extracted from a JSON ABI.
#[derive(Debug, Clone)]
pub struct EvmAbiEvent {
    /// Event name (e.g. "Swap").
    pub name: String,
    /// Human-readable signature with names and indexed markers
    /// (e.g. "Swap(address indexed sender, address indexed recipient, int256 amount0, ...)").
    /// Can be passed directly to [`decode_events`].
    pub signature: String,
    /// Canonical selector signature without names
    /// (e.g. "Swap(address,address,int256,int256,uint160,uint128,int24)").
    pub selector_signature: String,
    /// topic0 as 0x-prefixed hex string.
    pub topic0: String,
}

/// Parsed function info extracted from a JSON ABI.
#[derive(Debug, Clone)]
pub struct EvmAbiFunction {
    /// Function name (e.g. "swap").
    pub name: String,
    /// Human-readable signature with names
    /// (e.g. "swap(address recipient, bool zeroForOne, int256 amountSpecified, ...)").
    pub signature: String,
    /// Canonical selector signature without names
    /// (e.g. "swap(address,bool,int256,uint160,bytes)").
    pub selector_signature: String,
    /// 4-byte selector as 0x-prefixed hex string.
    pub selector: String,
}

/// Formats an event's full signature without the "event " prefix.
/// Produces e.g. "Swap(address indexed sender, address indexed recipient, int256 amount0)"
fn event_decode_signature(event: &alloy_json_abi::Event) -> String {
    let full = event.full_signature();
    // full_signature() returns "event Name(...)", strip the "event " prefix
    full.strip_prefix("event ").unwrap_or(&full).to_string()
}

/// Parse a JSON ABI string and extract all events.
pub fn abi_events(json_str: &str) -> Result<Vec<EvmAbiEvent>> {
    let abi: JsonAbi = serde_json::from_str(json_str).context("parse JSON ABI")?;
    let mut events = Vec::new();
    for event in abi.events() {
        let selector = event.selector();
        events.push(EvmAbiEvent {
            name: event.name.clone(),
            signature: event_decode_signature(event),
            selector_signature: event.signature(),
            topic0: format!("0x{}", faster_hex::hex_string(selector.as_slice())),
        });
    }
    Ok(events)
}

/// Parse a JSON ABI string and extract all functions.
pub fn abi_functions(json_str: &str) -> Result<Vec<EvmAbiFunction>> {
    let abi: JsonAbi = serde_json::from_str(json_str).context("parse JSON ABI")?;
    let mut functions = Vec::new();
    for func in abi.functions() {
        let selector = func.selector();
        functions.push(EvmAbiFunction {
            name: func.name.clone(),
            signature: func.full_signature(),
            selector_signature: func.signature(),
            selector: format!("0x{}", faster_hex::hex_string(selector.as_slice())),
        });
    }
    Ok(functions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_overflow_with_allow_decode_fail() {
        // When decoding all pool events without topic filtering, a Swap decoder
        // may successfully ABI-decode body data from a different event type,
        // producing an int24 value that doesn't fit in i32. With allow_decode_fail=true
        // this should produce null instead of erroring.
        let sol_values = vec![Some(DynSolValue::Int(I256::MAX, 24))];
        let result = to_int_impl::<Int32Type>(24, &sol_values, true);
        assert!(result.is_ok());
        let arr = result.unwrap();
        assert!(arr.is_null(0));

        // Without allow_decode_fail, it should error
        let sol_values = vec![Some(DynSolValue::Int(I256::MAX, 24))];
        let result = to_int_impl::<Int32Type>(24, &sol_values, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_topic0_filtering_with_allow_decode_fail() {
        use arrow::array::GenericBinaryBuilder;

        let swap_sig = "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)";
        let mint_sig = "Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)";

        let swap_selector = signature_to_topic0(swap_sig).unwrap();
        let mint_selector = signature_to_topic0(mint_sig).unwrap();

        // Build a batch with 2 rows: one Swap event and one Mint event
        let mut topic0_builder = GenericBinaryBuilder::<i32>::new();
        let mut topic1_builder = GenericBinaryBuilder::<i32>::new();
        let mut topic2_builder = GenericBinaryBuilder::<i32>::new();
        let mut topic3_builder = GenericBinaryBuilder::<i32>::new();
        let mut data_builder = GenericBinaryBuilder::<i32>::new();

        let addr = [0u8; 32];

        // Row 0: Swap event
        topic0_builder.append_value(swap_selector);
        topic1_builder.append_value(addr);
        topic2_builder.append_value(addr);
        topic3_builder.append_null();
        let amount0 = I256::try_from(-1000i64).unwrap();
        let amount1 = I256::try_from(2000i64).unwrap();
        let sqrt_price: U256 = U256::from(1u64) << 96;
        let liquidity = U256::from(1000000u64);
        let tick = I256::try_from(-100i64).unwrap();
        let mut swap_body = Vec::new();
        swap_body.extend_from_slice(&amount0.to_be_bytes::<32>());
        swap_body.extend_from_slice(&amount1.to_be_bytes::<32>());
        swap_body.extend_from_slice(&sqrt_price.to_be_bytes::<32>());
        swap_body.extend_from_slice(&liquidity.to_be_bytes::<32>());
        swap_body.extend_from_slice(&tick.to_be_bytes::<32>());
        data_builder.append_value(&swap_body);

        // Row 1: Mint event (different topic0, different body layout)
        topic0_builder.append_value(mint_selector);
        topic1_builder.append_value(addr);
        topic2_builder.append_value(addr);
        topic3_builder.append_value(addr);
        let mint_body = vec![0u8; 32 * 4]; // sender, amount, amount0, amount1
        data_builder.append_value(&mint_body);

        let schema = Arc::new(Schema::new(vec![
            Field::new("topic0", DataType::Binary, true),
            Field::new("topic1", DataType::Binary, true),
            Field::new("topic2", DataType::Binary, true),
            Field::new("topic3", DataType::Binary, true),
            Field::new("data", DataType::Binary, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(topic0_builder.finish()),
                Arc::new(topic1_builder.finish()),
                Arc::new(topic2_builder.finish()),
                Arc::new(topic3_builder.finish()),
                Arc::new(data_builder.finish()),
            ],
        )
        .unwrap();

        // With filter_by_topic0=true, should filter to only the Swap row
        let result = decode_events(swap_sig, &batch, true, true, false).unwrap();
        assert_eq!(result.num_rows(), 1, "should only decode the Swap row");

        // With filter_by_topic0=true and hstack=true, decoded + input columns are returned
        let result = decode_events(swap_sig, &batch, true, true, true).unwrap();
        assert_eq!(result.num_rows(), 1);
        // Should have decoded columns + original input columns
        assert!(
            result.column_by_name("topic0").is_some(),
            "hstack should include original input columns"
        );
    }

    #[test]
    fn test_abi_events() {
        let abi_json = r#"[
            {
                "anonymous": false,
                "inputs": [
                    {"indexed": true, "internalType": "address", "name": "sender", "type": "address"},
                    {"indexed": true, "internalType": "address", "name": "recipient", "type": "address"},
                    {"indexed": false, "internalType": "int256", "name": "amount0", "type": "int256"},
                    {"indexed": false, "internalType": "int256", "name": "amount1", "type": "int256"},
                    {"indexed": false, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
                    {"indexed": false, "internalType": "uint128", "name": "liquidity", "type": "uint128"},
                    {"indexed": false, "internalType": "int24", "name": "tick", "type": "int24"}
                ],
                "name": "Swap",
                "type": "event"
            },
            {
                "inputs": [{"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"}],
                "name": "initialize",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            }
        ]"#;

        let events = abi_events(abi_json);
        assert!(events.is_ok());
        let events = events.unwrap_or_default();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "Swap");
        assert_eq!(
            events[0].signature,
            "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
        );
        assert_eq!(
            events[0].selector_signature,
            "Swap(address,address,int256,int256,uint160,uint128,int24)"
        );
        assert!(events[0].topic0.starts_with("0x"));
        // Verify the signature works with decode (parse round-trip)
        let topic0_from_sig = signature_to_topic0(&events[0].signature);
        assert!(topic0_from_sig.is_ok());
    }

    #[test]
    fn test_abi_functions() {
        let abi_json = r#"[
            {
                "inputs": [
                    {"internalType": "address", "name": "recipient", "type": "address"},
                    {"internalType": "bool", "name": "zeroForOne", "type": "bool"},
                    {"internalType": "int256", "name": "amountSpecified", "type": "int256"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                    {"internalType": "bytes", "name": "data", "type": "bytes"}
                ],
                "name": "swap",
                "outputs": [
                    {"internalType": "int256", "name": "amount0", "type": "int256"},
                    {"internalType": "int256", "name": "amount1", "type": "int256"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "anonymous": false,
                "inputs": [
                    {"indexed": true, "internalType": "address", "name": "sender", "type": "address"}
                ],
                "name": "Swap",
                "type": "event"
            }
        ]"#;

        let functions = abi_functions(abi_json);
        assert!(functions.is_ok());
        let functions = functions.unwrap_or_default();
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].name, "swap");
        assert_eq!(
            functions[0].selector_signature,
            "swap(address,bool,int256,uint160,bytes)"
        );
        assert!(functions[0].selector.starts_with("0x"));
        assert_eq!(functions[0].selector.len(), 10); // "0x" + 8 hex chars
    }

    #[test]
    fn test_abi_events_empty_abi() {
        let events = abi_events("[]");
        assert!(events.is_ok());
        assert!(events.unwrap_or_default().is_empty());
    }

    #[test]
    #[ignore]
    fn nested_event_signature_to_schema() {
        let sig = "ConfiguredQuests(address editor, uint256[][], address indexed my_addr, (bool,bool[],(bool, uint256[]))[] questDetails)";

        let schema = event_signature_to_arrow_schema(sig).unwrap();

        let expected_schema = Schema::new(vec![
            Arc::new(Field::new("my_addr", DataType::Binary, true)),
            Arc::new(Field::new("editor", DataType::Binary, true)),
            Arc::new(Field::new(
                "param1",
                DataType::List(Arc::new(Field::new(
                    "",
                    DataType::List(Arc::new(Field::new("", DataType::Decimal256(76, 0), true))),
                    true,
                ))),
                true,
            )),
            Arc::new(Field::new(
                "questDetails",
                DataType::List(Arc::new(Field::new(
                    "",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(Field::new("param0", DataType::Boolean, true)),
                        Arc::new(Field::new(
                            "param1",
                            DataType::List(Arc::new(Field::new("", DataType::Boolean, true))),
                            true,
                        )),
                        Arc::new(Field::new(
                            "param2",
                            DataType::Struct(Fields::from(vec![
                                Arc::new(Field::new("param0", DataType::Boolean, true)),
                                Arc::new(Field::new(
                                    "param1",
                                    DataType::List(Arc::new(Field::new(
                                        "",
                                        DataType::Decimal256(76, 0),
                                        true,
                                    ))),
                                    true,
                                )),
                            ])),
                            true,
                        )),
                    ])),
                    true,
                ))),
                true,
            )),
        ]);

        assert_eq!(schema, expected_schema);
    }

    #[test]
    #[ignore]
    fn i256_to_arrow_i256() {
        for val in [
            I256::MIN,
            I256::MAX,
            I256::MAX / I256::try_from(2i32).unwrap(),
        ] {
            let out = arrow::datatypes::i256::from_be_bytes(val.to_be_bytes::<32>());

            assert_eq!(val.to_string(), out.to_string());
        }
    }

    #[test]
    #[ignore]
    fn read_parquet_with_real_data() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open("logs.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let logs = reader.next().unwrap().unwrap();

        let signature =
            "PairCreated(address indexed token0, address indexed token1, address pair,uint256)";

        let decoded = decode_events(signature, &logs, false, false, false).unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("decoded_logs.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, decoded.schema(), None).unwrap();
        writer.write(&decoded).unwrap();
        writer.close().unwrap();
    }
}
