use crate::internal::resp::RespValue;
use crate::internal::traits::RespVisitor;

use std::collections::HashMap;

pub struct CommandExecutor {
    data: HashMap<String, String>,
}

impl CommandExecutor {
    pub fn new() -> Self {
        CommandExecutor {
            data: HashMap::new(),
        }
    }
}

impl RespVisitor for CommandExecutor {
    fn visit_array(&mut self, array: &Vec<RespValue>) -> RespValue {
        if array.is_empty() {
            return RespValue::Error(String::from("Empty command array"));
        }
        let cmd_name = match &array[0] {
            RespValue::BulkString(b) => String::from_utf8_lossy(b).to_uppercase(),
            _ => return RespValue::Error(String::from("Command must be a bulk string")),
        };

        match cmd_name.as_str() {
            "PING" => RespValue::SimpleString(String::from("PONG")),
            "ECHO" => match &array[1] {
                RespValue::BulkString(b) => RespValue::BulkString(b.clone()),
                _ => return RespValue::Error(String::from("expected ECHO <bulkstring>")),
            },
            "SET" => {
                let key = match &array[1] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("key must be bulkstring")),
                };
                let val = match &array[2] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("value must be bulkstring")),
                };
                self.data
                    .entry(key)
                    .and_modify(|v: &mut String| *v = val.clone())
                    .or_insert(val);
                RespValue::SimpleString(String::from("OK"))
            }
            "GET" => {
                let key = match &array[1] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("key must be bulkstring")),
                };
                println!("{:?}", self.data);
                match self.data.get(&key) {
                    Some(val) => RespValue::BulkString(val.clone().into_bytes()),
                    None => RespValue::Null,
                }
            }
            _ => RespValue::Error(format!("Unknown command: {}", cmd_name)),
        }
    }
}
