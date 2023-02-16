use std::sync::Arc;

#[derive(Debug)]
pub enum RedisValue {
  Ok,
  Nothing,
  Integer(i64),
  EmptyString,
  SimpleString(Arc<Vec<u8>>),
  BulkString(Vec<String>),
  Array(Vec<String>),
}

impl From<&'static str> for RedisValue {
  fn from(value: &'static str) -> Self {
    RedisValue::SimpleString(Arc::new(value.as_bytes().to_vec()))
  }
}

impl From<usize> for RedisValue {
  fn from(value: usize) -> Self {
    RedisValue::Integer(value as i64)
  }
}

impl From<Vec<Vec<u8>>> for RedisValue {
  fn from(value: Vec<Vec<u8>>) -> Self {
    RedisValue::Array(
      value
        .iter()
        .map(|v| String::from_utf8(v.to_vec()).unwrap())
        .collect::<Vec<_>>(),
    )
  }
}
