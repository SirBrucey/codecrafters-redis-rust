use std::io;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

mod command;
mod parse;

use parse::RespSerialise;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { process(socket).await });
    }
}

async fn process(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buf) {
            Ok(0) => break,
            Ok(_n) => {
                let (_, elem) = parse::parse_element(&buf).unwrap();
                dbg!(&elem);
                let cmd: Result<command::Command, command::CommandError> = elem.try_into();
                let resp = match cmd {
                    Ok(cmd) => cmd.execute().serialise(),
                    Err(_e) => {
                        parse::SimpleError::from("Unable to parse input into command".to_owned())
                            .serialise()
                    }
                };
                stream.write_all(&resp).await.unwrap();
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{}", e),
        }
    }
}
