use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

mod internal;
use crate::internal::cmd::CommandExecutor;
use crate::internal::resp::{self, RespValue};

fn execute_cmd(command: RespValue) -> RespValue {
    let mut executor = CommandExecutor;
    command.accept(&mut executor)
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    match stream.read(&mut buffer) {
        Ok(bytes_read) if bytes_read > 0 => {
            let raw_data = &buffer[..bytes_read];
            match resp::parse(raw_data) {
                Ok(command) => {
                    println!("Parsed command: {:?}", command);
                    let response = execute_cmd(command);
                    let response_bytes = match response {
                        RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
                        RespValue::Error(e) => format!("-{}\r\n", e).into_bytes(),
                        _ => unimplemented!(),
                    };
                    let _ = stream.write_all(&response_bytes);
                }
                Err(e) => {
                    eprintln!("Parsing error: {}", e);
                    let _ = stream.write_all(format!("-ERR {}\r\n", e).as_bytes());
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to read from  socket: {}", e);
        }
        _ => {}
    }
}

fn main() -> std::io::Result<()> {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(|| {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
