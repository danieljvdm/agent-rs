#![allow(dead_code)]
use super::openai_wire::Api;
use serde_json::Value;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};
pub fn accept(listener: &TcpListener) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match listener.accept() {
            Ok((socket, _)) => {
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return socket;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(1))
            }
            Err(error) => panic!("fixture accept: {error}"),
        }
    }
}

pub fn request(socket: &mut TcpStream, api: Api) -> Value {
    let mut bytes = Vec::new();
    let (header_end, length) = loop {
        let mut buffer = [0; 4096];
        let n = socket.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        assert!(bytes.len() < 1_048_576);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            // Never include headers in failure messages (they contain the test key).
            assert!(headers.starts_with(&format!("POST {} HTTP/1.1", api.path())));
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|n| n.trim().parse().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + length {
        let mut buffer = [0; 4096];
        let n = socket.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}

pub fn serve(api: Api, bodies: Vec<String>) -> (String, thread::JoinHandle<Vec<Value>>) {
    serve_status(api, bodies, "200 OK", "text/event-stream")
}

pub fn serve_status(
    api: Api,
    bodies: Vec<String>,
    status: &'static str,
    content_type: &'static str,
) -> (String, thread::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = thread::spawn(move || {
        let mut requests = vec![];
        for body in bodies {
            let mut socket = accept(&listener);
            requests.push(request(&mut socket, api));
            write!(socket, "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            // Rejection/cancellation may close the connection while writing.
            for chunk in body.as_bytes().chunks(127) {
                if socket.write_all(chunk).is_err() {
                    break;
                }
            }
        }
        requests
    });
    (format!("http://{address}/v1"), thread)
}
