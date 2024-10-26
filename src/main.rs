use clap::Parser;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

mod command;
mod parse;

use parse::RespSerialise;

#[derive(Debug, Parser)]
pub(crate) struct Opts {
    #[clap(short, long, default_value = "6379")]
    port: u16,
    #[clap(short, long, default_value = "/tmp/redis-data")]
    dir: PathBuf,
    #[clap(short, long, default_value = "rdbfile")]
    dbfilename: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", opts.port)).await?;

    let opts = Arc::new(load_opts(opts));
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        let opts = opts.clone();
        tokio::spawn(async move { process(socket, db, opts).await });
    }
}

async fn process(
    mut stream: TcpStream,
    db: Arc<Mutex<HashMap<String, command::DbValue>>>,
    opts: Arc<HashMap<String, OptValue>>,
) {
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
                    Ok(cmd) => cmd.execute(&db, &opts).serialise(),
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

enum OptValue {
    String(String),
    UInt(u16),
    Path(PathBuf),
}

fn load_opts(opts: Opts) -> HashMap<String, OptValue> {
    let mut map = HashMap::new();
    map.insert("port".to_owned(), OptValue::UInt(opts.port));
    map.insert("dir".to_owned(), OptValue::Path(opts.dir));
    map.insert("dbfilename".to_owned(), OptValue::String(opts.dbfilename));
    map
}
