use std::sync::{Arc, LazyLock};

use arrow_schema::DataType;
use common_error::{DaftError, DaftResult};
use serde_arrow::{
    schema::{SchemaLike, TracingOptions},
    utils::{Item, Items},
};
use sketches_ddsketch::DDSketch;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to deserialize from arrow"))]
    DeserializationError { source: serde_arrow::Error },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        use Error::DeserializationError;
        match value {
            DeserializationError { source } => {
                Self::ComputeError(format!("Deserialization error: {source}"))
            }
        }
    }
}

// Expected to be a vector of length 1
static ARROW_DDSKETCH_ITEM_FIELDS: LazyLock<Vec<arrow_schema_59::FieldRef>> = LazyLock::new(|| {
    Vec::<arrow_schema_59::FieldRef>::from_type::<Item<Option<DDSketch>>>(TracingOptions::default())
        .unwrap()
});

/// The corresponding Arrow DataType of Vec<DDSketch> when serialized as an Arrow array
pub static ARROW_DDSKETCH_DTYPE: LazyLock<arrow_schema::DataType> = LazyLock::new(|| {
    let schema =
        arrow_schema_59::ffi::FFI_ArrowSchema::try_from(ARROW_DDSKETCH_ITEM_FIELDS[0].as_ref())
            .unwrap();
    // SAFETY: both types implement the Arrow C Data Interface ABI. The schema
    // was exported by Arrow and stays alive for the entire borrowed import.
    let schema =
        unsafe { &*std::ptr::from_ref(&schema).cast::<arrow_schema::ffi::FFI_ArrowSchema>() };
    arrow_schema::DataType::try_from(schema).unwrap()
});

static ARROW_DDSKETCH_FIELDS: LazyLock<arrow_schema::Fields> = LazyLock::new(|| {
    let DataType::Struct(fields) = ARROW_DDSKETCH_DTYPE.clone() else {
        panic!("Expected StructDataType");
    };

    fields
});

/// Converts a Vec<Option<DDSketch>> into an Arrow Array
#[must_use]
pub fn into_arrow(sketches: Vec<Option<DDSketch>>) -> arrow_array::ArrayRef {
    if sketches.is_empty() {
        return Arc::new(arrow_array::StructArray::new_null(
            ARROW_DDSKETCH_FIELDS.clone(),
            0,
        ));
    }

    let wrapped_sketches: Items<Vec<Option<DDSketch>>> = Items(sketches);
    let mut arrow_arrays =
        serde_arrow::to_arrow(ARROW_DDSKETCH_ITEM_FIELDS.as_slice(), &wrapped_sketches).unwrap();

    import_from_serde_arrow(arrow_arrays.pop().unwrap()).unwrap()
}

/// Converts an Arrow Array into a Vec<Option<DDSketch>>
pub fn from_arrow(arrow_array: arrow_array::ArrayRef) -> DaftResult<Vec<Option<DDSketch>>> {
    if arrow_array.is_empty() {
        return Ok(vec![]);
    }

    let arrow_array = export_to_serde_arrow(arrow_array)?;
    let item_vec = serde_arrow::from_arrow::<Vec<Item<Option<DDSketch>>>, _>(
        &ARROW_DDSKETCH_ITEM_FIELDS,
        &[arrow_array],
    );
    item_vec
        .map(|item_vec| item_vec.into_iter().map(|item| item.0).collect())
        .with_context(|_| DeserializationSnafu {})
        .map_err(std::convert::Into::into)
}

// serde_arrow currently supports Arrow <= 59. The stable C ABI transfers buffer
// ownership between versions without serializing IPC or copying array buffers.
// Remove this adapter when serde_arrow supports the workspace Arrow version.
const _: () = {
    assert!(
        std::mem::size_of::<arrow_array::ffi::FFI_ArrowArray>()
            == std::mem::size_of::<arrow_array_59::ffi::FFI_ArrowArray>()
    );
    assert!(
        std::mem::align_of::<arrow_array::ffi::FFI_ArrowArray>()
            == std::mem::align_of::<arrow_array_59::ffi::FFI_ArrowArray>()
    );
    assert!(
        std::mem::size_of::<arrow_schema::ffi::FFI_ArrowSchema>()
            == std::mem::size_of::<arrow_schema_59::ffi::FFI_ArrowSchema>()
    );
    assert!(
        std::mem::align_of::<arrow_schema::ffi::FFI_ArrowSchema>()
            == std::mem::align_of::<arrow_schema_59::ffi::FFI_ArrowSchema>()
    );
};

fn import_from_serde_arrow(array: arrow_array_59::ArrayRef) -> DaftResult<arrow_array::ArrayRef> {
    let mut ffi_array = arrow_array_59::ffi::FFI_ArrowArray::new(&array.to_data());
    let ffi_schema = arrow_schema_59::ffi::FFI_ArrowSchema::try_from(array.data_type())
        .map_err(|e| DaftError::TypeError(e.to_string()))?;
    // SAFETY: the layout is the standard C ABI (checked above), and Arrow's
    // exporter created a valid array/schema pair. from_raw moves ownership and
    // empties ffi_array; the imported array retains its original release callback.
    let data = unsafe {
        let array =
            arrow_array::ffi::FFI_ArrowArray::from_raw(std::ptr::from_mut(&mut ffi_array).cast());
        let schema = &*std::ptr::from_ref(&ffi_schema).cast::<arrow_schema::ffi::FFI_ArrowSchema>();
        arrow_array::ffi::from_ffi(array, schema)
    }?;
    Ok(arrow_array::make_array(data))
}

fn export_to_serde_arrow(array: arrow_array::ArrayRef) -> DaftResult<arrow_array_59::ArrayRef> {
    let mut ffi_array = arrow_array::ffi::FFI_ArrowArray::new(&array.to_data());
    let ffi_schema = arrow_schema::ffi::FFI_ArrowSchema::try_from(array.data_type())?;
    // SAFETY: same C ABI ownership transfer as import_from_serde_arrow, in the
    // opposite direction. The borrowed schema lives until from_ffi returns.
    let data = unsafe {
        let array = arrow_array_59::ffi::FFI_ArrowArray::from_raw(
            std::ptr::from_mut(&mut ffi_array).cast(),
        );
        let schema =
            &*std::ptr::from_ref(&ffi_schema).cast::<arrow_schema_59::ffi::FFI_ArrowSchema>();
        arrow_array_59::ffi::from_ffi(array, schema)
    }
    .map_err(|e| DaftError::TypeError(e.to_string()))?;
    Ok(arrow_array_59::make_array(data))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array};
    use common_error::DaftResult;
    use sketches_ddsketch::{Config, DDSketch};

    use crate::{from_arrow, into_arrow};

    #[test]
    fn test_c_data_bridge_preserves_slices_and_ownership() -> DaftResult<()> {
        let original = Int64Array::from(vec![Some(10), None, Some(30), Some(40)]).slice(1, 2);
        let values_address = original.values().as_ptr();
        let foreign = super::export_to_serde_arrow(Arc::new(original))?;
        // Both owning arrays are moved into the bridge. Only the imported
        // release callback retains the buffers by the time we inspect them.
        let restored = super::import_from_serde_arrow(foreign)?;
        let restored = restored.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(restored.values().as_ptr(), values_address);
        assert_eq!(restored.len(), 2);
        assert!(restored.is_null(0));
        assert_eq!(restored.value(1), 30);
        Ok(())
    }

    #[test]
    fn test_roundtrip_single() -> DaftResult<()> {
        let mut sketch = DDSketch::new(Config::default());

        for i in 0..10 {
            sketch.add(f64::from(i));
        }

        let expected_min = sketch.min();
        let expected_max = sketch.max();
        let expected_sum = sketch.sum();
        let expected_count = sketch.count();
        let expected_length = sketch.length();
        let expected_quantile = sketch.quantile(0.5);

        let sketches = vec![Some(sketch)];
        let mut round_tripped = from_arrow(into_arrow(sketches))?;

        assert_eq!(round_tripped.len(), 1);
        let received = round_tripped.pop().unwrap().unwrap();
        assert_eq!(received.min(), expected_min);
        assert_eq!(received.max(), expected_max);
        assert_eq!(received.sum(), expected_sum);
        assert_eq!(received.count(), expected_count);
        assert_eq!(received.length(), expected_length);
        assert_eq!(received.quantile(0.5).unwrap(), expected_quantile.unwrap());

        Ok(())
    }

    #[test]
    fn test_roundtrip_null() -> DaftResult<()> {
        let sketches = vec![None];
        let mut round_tripped = from_arrow(into_arrow(sketches))?;
        assert_eq!(round_tripped.len(), 1);
        assert!(round_tripped.pop().unwrap().is_none());
        Ok(())
    }

    #[test]
    fn test_roundtrip_some_null() -> DaftResult<()> {
        let sketches = vec![Some(DDSketch::new(Config::default())), None];
        let mut round_tripped = from_arrow(into_arrow(sketches))?;
        assert_eq!(round_tripped.len(), 2);

        assert!(round_tripped.pop().unwrap().is_none());
        assert!(round_tripped.pop().unwrap().is_some());
        Ok(())
    }

    #[test]
    fn test_roundtrip_empty() -> DaftResult<()> {
        let sketches = vec![];
        let round_tripped = from_arrow(into_arrow(sketches))?;
        assert_eq!(round_tripped.len(), 0);
        Ok(())
    }
}
