use crate::internal::traits::RespVisitor;

#[derive(Debug, Eq, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
}

impl RespValue {
    pub fn accept<V: RespVisitor>(&self, visitor: &mut V) -> RespValue {
        match self {
            RespValue::Array(a) => visitor.visit_array(a),
            RespValue::BulkString(b) => visitor.visit_bulk_string(b),
            _ => unimplemented!(),
        }
    }
}

impl std::fmt::Display for RespValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespValue::SimpleString(s) => {
                write!(f, "+{}\r\n", s)
            }
            RespValue::Error(e) => {
                write!(f, "-{}\r\n", e)
            }
            RespValue::Integer(i) => {
                write!(f, ":{}\r\n", i)
            }
            RespValue::BulkString(bytes) => {
                write!(f, "${}\r\n", bytes.len())?;
                let data_str = String::from_utf8_lossy(bytes);
                write!(f, "{}\r\n", data_str)
            }
            RespValue::Array(elements) => {
                write!(f, "*{}\r\n", elements.len())?;
                for element in elements {
                    write!(f, "{}", element)?;
                }
                Ok(())
            }
            RespValue::Null => {
                write!(f, "$-1\r\n")
            }
        }
    }
}

fn consume_crlf(data: &[u8], offset: &mut usize) -> Result<(), String> {
    if data.len() < *offset + 2 || &data[*offset..*offset + 2] != b"\r\n" {
        return Err(String::from("Missing CRLF terminator"));
    }
    *offset += 2;
    Ok(())
}

fn parse_integer(data: &[u8], offset: &mut usize) -> Result<i64, String> {
    let start = *offset;
    while *offset < data.len() && data[*offset] != b'\r' {
        *offset += 1;
    }
    let num_bytes = &data[start..*offset];
    let num_str = std::str::from_utf8(num_bytes).map_err(|_| String::from("Invalid utf-8"))?;

    let number = num_str
        .parse::<i64>()
        .map_err(|_| format!("Invalid integer format, {}", num_str))?;

    consume_crlf(data, offset)?;
    Ok(number)
}

fn parse_bulk_string(data: &[u8], offset: &mut usize) -> Result<RespValue, String> {
    if data.get(*offset) != Some(&b'$') {
        return Err(String::from("Expected bulk string prefix '$'"));
    }
    *offset += 1;
    let length = parse_integer(data, offset)?;

    if length < 0 {
        return Ok(RespValue::Null);
    }
    let length = length as usize;
    if data.len() < *offset + length {
        return Err(String::from("Incomplete bulk string data"));
    }
    let bulk_data = data[*offset..*offset + length].to_vec();
    *offset += length;
    consume_crlf(data, offset)?;
    Ok(RespValue::BulkString(bulk_data))
}

pub fn parse(data: &[u8], offset: &mut usize) -> Result<RespValue, String> {
    match data.get(*offset).ok_or("Empty data")? {
        b'*' => {
            *offset += 1;
            let num_elements = parse_integer(data, offset)?;

            if num_elements < 1 {
                return Err(String::from("Expected a command array of length atleast 1"));
            }
            let mut elements = Vec::with_capacity(num_elements as usize);
            for _ in 0..num_elements {
                elements.push(parse(&data, offset)?);
            }
            return Ok(RespValue::Array(elements));
        }
        b'$' => parse_bulk_string(data, offset),
        _ => Err(String::from("Unsupported or invalid RESP prefix")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_ping_str() {
        let ping_cmd = String::from("*1\r\n$4\r\nPING\r\n");
        let ping_bytes = ping_cmd.as_bytes();
        let mut offset = 0;
        assert_eq!(
            parse(ping_bytes, &mut offset).unwrap(),
            RespValue::Array(vec![RespValue::BulkString(vec![80, 73, 78, 71])])
        ); // P   I   N   G
    }

    #[test]
    fn parse_echo_str() {
        let echo_cmd = String::from("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
        let echo_bytes = echo_cmd.as_bytes();
        let mut offset = 0;
        assert_eq!(
            parse(echo_bytes, &mut offset).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(vec![69, 67, 72, 79]),
                RespValue::BulkString(vec![104, 101, 121])
            ])
        );
    }

    #[test]
    fn parse_set_str() {
        let set_cmd = String::from("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let set_bytes = set_cmd.as_bytes();
        let mut offset = 0;
        assert_eq!(
            parse(set_bytes, &mut offset).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(vec![83, 69, 84]),
                RespValue::BulkString(vec![102, 111, 111]),
                RespValue::BulkString(vec![98, 97, 114])
            ])
        );
    }

    #[test]
    fn parse_get_str() {
        let get_cmd = String::from("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        let get_bytes = get_cmd.as_bytes();
        let mut offset = 0;
        assert_eq!(
            parse(get_bytes, &mut offset).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(vec![71, 69, 84]),
                RespValue::BulkString(vec![102, 111, 111]),
            ])
        );
    }

    #[test]
    fn parse_set_cmd_with_expiry() {
        let set_cmd = String::from(
            "*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n10\r\n",
        );
        let set_bytes = set_cmd.as_bytes();
        let mut offset = 0;
        assert_eq!(
            parse(set_bytes, &mut offset).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(vec![83, 69, 84]),
                RespValue::BulkString(vec![109, 121, 107, 101, 121]),
                RespValue::BulkString(vec![118, 97, 108, 117, 101]),
                RespValue::BulkString(vec![69, 88]),
                RespValue::BulkString(vec![49, 48]),
            ])
        );
    }

    #[test]
    fn parse_rpush_cmd() {
        let rpush_cmd = String::from("*3\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$3\r\nfoo\r\n");
        let rpush_bytes = rpush_cmd.as_bytes();
        let mut offset = 0;
        assert_eq!(
            parse(rpush_bytes, &mut offset).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(vec![82, 80, 85, 83, 72]),
                RespValue::BulkString(vec![108, 105, 115, 116, 95, 107, 101, 121]),
                RespValue::BulkString(vec![102, 111, 111]),
            ])
        );
    }
}
