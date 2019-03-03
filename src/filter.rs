use std::ops::Bound::{self, Excluded, Included, Unbounded};

use std::f64;
use std::i64;

use crate::unsafe_float::UnsafeFloat;

pub enum Operator {
    EQ,
    LT,
    LE,
    GT,
    GE,
    IN,
    PRE,
}

impl Operator {
    pub fn from(op: &str) -> Result<Self, &'static str> {
        match op.to_uppercase().as_ref() {
            "LT" => Ok(Operator::LT),
            "LE" => Ok(Operator::LE),
            "EQ" => Ok(Operator::EQ),
            "GE" => Ok(Operator::GE),
            "GT" => Ok(Operator::GT),
            "IN" => Ok(Operator::IN),
            "PRE" => Ok(Operator::PRE),
            _ => Err("Unknown operator"),
        }
    }
}

pub struct Filter<'a> {
    pub op: Operator,
    pub value: &'a str,
    pub value2: &'a str,
    pub column: usize,
}

impl<'a> Filter<'a> {
    pub fn string_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        match self.op {
            Operator::EQ => (
                Included(self.value.as_bytes().to_owned()),
                Included(self.value.as_bytes().to_owned()),
            ),
            Operator::LE => (Unbounded, Included(self.value.as_bytes().to_owned())),
            Operator::LT => (Unbounded, Excluded(self.value.as_bytes().to_owned())),
            Operator::GT => (Excluded(self.value.as_bytes().to_owned()), Unbounded),
            Operator::GE => (Included(self.value.as_bytes().to_owned()), Unbounded),
            Operator::IN => (
                Included(self.value.as_bytes().to_owned()),
                Included(self.value2.as_bytes().to_owned()),
            ),
            Operator::PRE => {
                let mut upper = self.value.as_bytes().to_owned();
                upper.append(&mut vec![255; 4]);
                (Included(self.value.as_bytes().to_owned()), Included(upper))
            }
        }
    }

    pub fn int_bounds(&self) -> (Bound<i64>, Bound<i64>) {
        let value: i64 = self.value.parse().unwrap_or(i64::MIN);

        match self.op {
            Operator::EQ => (Included(value), Included(value)),
            Operator::LE => (Excluded(i64::MIN), Included(value)),
            Operator::LT => (Excluded(i64::MIN), Excluded(value)),
            Operator::GT => (Excluded(value), Excluded(i64::MAX)),
            Operator::GE => (Included(value), Excluded(i64::MAX)),
            Operator::IN => {
                let value2: i64 = self.value2.parse().unwrap_or(i64::MIN);
                (Included(value), Included(value2))
            }
            Operator::PRE => panic!("unsupported operator for integer"),
        }
    }

    pub fn float_bounds(&self) -> (Bound<UnsafeFloat>, Bound<UnsafeFloat>) {
        let value = UnsafeFloat(self.value.parse().unwrap_or(f64::NEG_INFINITY));

        let lower = Excluded(UnsafeFloat(f64::NEG_INFINITY));
        let upper = Excluded(UnsafeFloat(f64::INFINITY));

        match self.op {
            Operator::EQ => (Included(value), Included(value)),
            Operator::LE => (lower, Included(value)),
            Operator::LT => (lower, Excluded(value)),
            Operator::GT => (Excluded(value), upper),
            Operator::GE => (Included(value), upper),
            Operator::IN => {
                let value2 = UnsafeFloat(self.value2.parse().unwrap_or(f64::NEG_INFINITY));
                (Included(value), Included(value2))
            }
            Operator::PRE => panic!("unsupported operator for float"),
        }
    }
}
