use crate::internal::resp::RespValue;
use crate::internal::traits::RespVisitor;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

pub enum Expiration {
    Seconds(u64),
    Milliseconds(u64),
}

#[derive(Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
}

#[derive(Clone)]
pub struct ValueEntry {
    pub value: RedisValue,
    pub expiry_time: Option<SystemTime>,
}

pub struct CommandExecutor {
    data: HashMap<String, ValueEntry>,
}

impl CommandExecutor {
    pub fn new() -> Self {
        CommandExecutor {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String, expiry_opt: Option<Expiration>) {
        let expiry_time = expiry_opt.map(|arg| {
            let duration = match arg {
                Expiration::Seconds(s) => Duration::from_secs(s),
                Expiration::Milliseconds(s) => Duration::from_millis(s),
            };
            SystemTime::now() + duration
        });
        let entry = ValueEntry {
            value: RedisValue::String(value),
            expiry_time,
        };
        self.data
            .entry(key)
            .and_modify(|v: &mut ValueEntry| *v = entry.clone())
            .or_insert(entry);
    }

    pub fn rpush(&mut self, key: String, values: Vec<String>, expiry_opt: Option<Expiration>) {
        let expiry_time = expiry_opt.map(|arg| {
            let duration = match arg {
                Expiration::Seconds(s) => Duration::from_secs(s),
                Expiration::Milliseconds(s) => Duration::from_millis(s),
            };
            SystemTime::now() + duration
        });

        for value in values {
            self.data
                .entry(key.clone())
                .and_modify(|v: &mut ValueEntry| match v.value {
                    RedisValue::List(ref mut buf) => buf.push(value.clone()),
                    _ => unreachable!("Inconsitent list format"),
                })
                .or_insert(ValueEntry {
                    value: RedisValue::List(vec![value]),
                    expiry_time: expiry_time,
                });
        }
    }

    pub fn lpush(&mut self, key: String, values: Vec<String>, expiry_opt: Option<Expiration>) {
        let expiry_time = expiry_opt.map(|arg| {
            let duration = match arg {
                Expiration::Seconds(s) => Duration::from_secs(s),
                Expiration::Milliseconds(s) => Duration::from_millis(s),
            };
            SystemTime::now() + duration
        });

        self.data
            .entry(key.clone())
            .and_modify(|v: &mut ValueEntry| match v.value {
                RedisValue::List(ref mut buf) => {
                    buf.splice(0..0, values.clone());
                }
                _ => unreachable!("Inconsitent list format"),
            })
            .or_insert(ValueEntry {
                value: RedisValue::List(values),
                expiry_time: expiry_time,
            });
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        let current_time = SystemTime::now();

        if let Some(entry) = self.data.get(&key) {
            if let Some(expiry) = entry.expiry_time {
                if current_time >= expiry {
                    self.data.remove(&key);
                    return None;
                }
            }
            match entry.value {
                RedisValue::String(ref val) => {
                    return Some(val.clone());
                }
                RedisValue::List(_) => {
                    unimplemented!()
                }
            };
        }
        None
    }

    pub fn llen(&self, lst_key: String) -> i64 {
        match self.data.get(&lst_key) {
            Some(val_entry) => match val_entry.value {
                RedisValue::List(ref buffer) => buffer.len() as i64,
                RedisValue::String(_) => unreachable!("llen is expected to apply for list only"),
            },
            None => 0,
        }
    }

    pub fn lrange(&self, lst_key: String, start: isize, end: isize) -> Vec<String> {
        match self.data.get(&lst_key) {
            Some(ventry) => match ventry.value {
                RedisValue::List(ref buffer) => {
                    let len = buffer.len() as isize;
                    let start_idx = if start < 0 {
                        isize::max(0, len + start)
                    } else {
                        isize::min(len - 1, start)
                    };

                    let end_idx = if end < 0 {
                        isize::max(0, len + end)
                    } else {
                        isize::min(len - 1, end)
                    };

                    if start_idx > end_idx {
                        return vec![];
                    }

                    let start_idx = start_idx as usize;
                    let end_idx = end_idx as usize;

                    let mut bucket = vec![];
                    for idx in start_idx..=end_idx {
                        bucket.push(buffer[idx].clone());
                    }
                    bucket
                }
                RedisValue::String(_) => {
                    unimplemented!("lrange is not supported for string val");
                }
            },
            None => {
                vec![]
            }
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
                let mut expiry_opt = None;
                if let Some(exp) = &array.get(3) {
                    let opt = match exp {
                        RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => return RespValue::Error(String::from("Either EX/PX expected")),
                    };
                    match opt.as_str() {
                        "EX" => {
                            if let Some(tsec_s) = &array.get(4) {
                                let tsec = match tsec_s {
                                    RespValue::BulkString(b) => {
                                        let ts = String::from_utf8_lossy(b).into_owned();
                                        ts.parse::<u64>().expect("Invalid int")
                                    }
                                    _ => {
                                        return RespValue::Error(String::from(
                                            "timeval must be bulkstring",
                                        ))
                                    }
                                };
                                expiry_opt = Some(Expiration::Seconds(tsec));
                            } else {
                                return RespValue::Error(String::from("EX <timeout-in-sec>"));
                            }
                        }
                        "PX" => {
                            if let Some(tmsec_s) = &array.get(4) {
                                let tmsec = match tmsec_s {
                                    RespValue::BulkString(b) => {
                                        let ts = String::from_utf8_lossy(b).into_owned();
                                        ts.parse::<u64>().expect("Invalid int")
                                    }
                                    _ => {
                                        return RespValue::Error(String::from(
                                            "timeval must be bulkstring",
                                        ))
                                    }
                                };
                                expiry_opt = Some(Expiration::Milliseconds(tmsec));
                            } else {
                                return RespValue::Error(String::from("PX <timeout-in-msec>"));
                            }
                        }
                        _ => return RespValue::Error(String::from("Either EX/PX supported")),
                    }
                }
                self.set(key, val, expiry_opt);
                RespValue::SimpleString(String::from("OK"))
            }
            "GET" => {
                let key = match &array[1] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("key must be bulkstring")),
                };
                match self.get(key) {
                    Some(val) => RespValue::BulkString(val.clone().into_bytes()),
                    None => RespValue::Null,
                }
            }
            "RPUSH" => {
                let lst_key = match &array[1] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("list name must be bulkstring")),
                };

                let mut buffer = vec![];
                for maybe_element in array.iter().skip(2) {
                    let element = match maybe_element {
                        RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => {
                            return RespValue::Error(String::from(
                                "list element must be bulkstring",
                            ))
                        }
                    };
                    buffer.push(element);
                }
                let expiry_opt = None; // TODO: add support for expiry for Rpush
                self.rpush(lst_key.clone(), buffer, expiry_opt);
                let lst_len = self.llen(lst_key);
                RespValue::Integer(lst_len)
            }
            "LPUSH" => {
                let lst_key = match &array[1] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("list name must be bulkstring")),
                };

                let mut buffer = vec![];
                for maybe_element in array.iter().skip(2) {
                    let element = match maybe_element {
                        RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => {
                            return RespValue::Error(String::from(
                                "list element must be bulkstring",
                            ))
                        }
                    };
                    buffer.push(element);
                }
                let expiry_opt = None; // TODO: add support for expiry for Rpush
                self.lpush(lst_key.clone(), buffer.into_iter().rev().collect(), expiry_opt);
                let lst_len = self.llen(lst_key);
                RespValue::Integer(lst_len)
            }
            "LRANGE" => {
                let lst_key = match &array[1] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    _ => return RespValue::Error(String::from("list name must be bulkstring")),
                };
                let start_idx = match &array[2] {
                    RespValue::BulkString(b) => {
                        let s_idx = String::from_utf8_lossy(b).into_owned();
                        s_idx.parse::<isize>().expect("invalid start index")
                    }
                    _ => return RespValue::Error(String::from("start_idx must be bulk string")),
                };
                let end_idx = match &array[3] {
                    RespValue::BulkString(b) => {
                        let e_idx = String::from_utf8_lossy(b).into_owned();
                        e_idx.parse::<isize>().expect("invalid end index")
                    }
                    _ => return RespValue::Error(String::from("start_idx must be bulk string")),
                };
                let buffer = self.lrange(lst_key, start_idx, end_idx);
                let mut bucket = vec![];
                for ele in buffer {
                    bucket.push(RespValue::BulkString(ele.into_bytes().to_vec()));
                }
                RespValue::Array(bucket)
            }
            _ => RespValue::Error(format!("Unknown command: {}", cmd_name)),
        }
    }
}
