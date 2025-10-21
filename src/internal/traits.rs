use crate::internal::resp::RespValue;

pub trait RespVisitor {
    fn visit_array(&mut self, array: &Vec<RespValue>) -> RespValue;
    fn visit_bulk_string(&mut self, bulk: &Vec<u8>) -> RespValue {
        RespValue::Null
    }
}
