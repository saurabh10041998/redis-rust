use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    match stream.read(&mut buffer) {
        Ok(bytes_read) => {
            let request = String::from_utf8_lossy(&buffer[..bytes_read]);
            let request = request.trim();
            println!("[+] Received request: {}", request);

            if request == String::from("PING") {
                let response_data = String::from("+PONG\r\n");
                let response_bytes = response_data.as_bytes();

                if let Err(e) = stream.write_all(response_bytes) {
                    eprintln!("[!!] Failed to send response: {}", e);
                }
            } else {
                eprintln!("[!!] I don't know how to respond to {}", request);
            }
        }
        Err(e) => {
            eprintln!("Failed to read from  socket: {}", e);
        }
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
