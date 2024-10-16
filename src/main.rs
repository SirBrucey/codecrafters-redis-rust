use std::io;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(socket).await
        });
    }
}

async fn process(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buf) {
            Ok(0) => break,
            Ok(_n) => {
                stream.write_all(b"+PONG\r\n").await.unwrap()
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{}", e)
        }
    }
}
