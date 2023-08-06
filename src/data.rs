use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum DType {
    String,
    Uint64,
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd)]
pub enum DValue {
    String(String),
    Uint64(u64),
}

pub fn get_dtype(value: &DValue) -> DType {
    match value {
        DValue::String(_) => DType::String,
        DValue::Uint64(_) => DType::Uint64,
    }
}

pub fn get_min<'a>(min_val: &'a DValue, new_val: &'a DValue) -> &'a DValue {
    match (min_val, new_val) {
        (DValue::String(min_s), DValue::String(new_s)) => {
            if new_s < min_s {
                new_val
            } else {
                min_val
            }
        }
        (DValue::Uint64(min_u), DValue::Uint64(new_u)) => {
            if new_u < min_u {
                new_val
            } else {
                min_val
            }
        }
        _ => panic!("Mismatched types"),
    }
}

pub fn get_max<'a>(max_val: &'a DValue, new_val: &'a DValue) -> &'a DValue {
    match (max_val, new_val) {
        (DValue::String(max_s), DValue::String(new_s)) => {
            if new_s > max_s {
                new_val
            } else {
                max_val
            }
        }
        (DValue::Uint64(max_u), DValue::Uint64(new_u)) => {
            if new_u > max_u {
                new_val
            } else {
                max_val
            }
        }
        _ => panic!("Mismatched types"),
    }
}