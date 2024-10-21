use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use crate::parse::RespElement;

pub(crate) enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: String,
        expiry: Option<u64>,
    },
}

pub(crate) struct DbValue {
    value: Bytes,
    expires_at: Option<std::time::Instant>,
}

impl Command {
    pub(crate) fn execute(self, db: &Arc<Mutex<HashMap<String, DbValue>>>) -> RespElement {
        match self {
            Self::Ping => RespElement::SimpleString("PONG".to_owned().into()),
            Self::Echo(message) => RespElement::BulkString(message.into()),
            Self::Get(key) => {
                let db = db.lock().unwrap();
                match db.get(&key) {
                    Some(db_value) => {
                        if let Some(expires_at) = db_value.expires_at {
                            if expires_at < std::time::Instant::now() {
                                return RespElement::Null;
                            }
                        }

                        RespElement::BulkString(db_value.value.clone().into())
                    }
                    None => RespElement::Null,
                }
            }
            Self::Set { key, value, expiry } => {
                let mut db = db.lock().unwrap();
                db.insert(
                    key,
                    DbValue {
                        value: value.into(),
                        expires_at: expiry.map(|expiry| {
                            std::time::Instant::now() + std::time::Duration::from_secs(expiry)
                        }),
                    },
                );
                RespElement::SimpleString("OK".to_owned().into())
            }
        }
    }
}

pub(crate) enum CommandError {
    MissingCommand,
    InvalidCommand,
    UnknownCommand,
}

impl TryFrom<RespElement> for Command {
    type Error = CommandError;

    fn try_from(element: RespElement) -> Result<Self, CommandError> {
        match element {
            RespElement::Array(elements) => {
                if elements.is_empty() {
                    return Err(CommandError::MissingCommand);
                }

                let command = &elements[0];
                match command {
                    RespElement::BulkString(command) => match command.as_str() {
                        "PING" => Ok(Command::Ping),
                        "ECHO" => {
                            if elements.len() != 2 {
                                return Err(CommandError::InvalidCommand);
                            }

                            let message = elements[1].clone();
                            match message {
                                RespElement::BulkString(message) => {
                                    Ok(Command::Echo(message.unwrap()))
                                }
                                _ => Err(CommandError::InvalidCommand),
                            }
                        }
                        "GET" => {
                            if elements.len() != 2 {
                                return Err(CommandError::InvalidCommand);
                            }

                            let key = elements[1].clone();
                            match key {
                                RespElement::BulkString(key) => Ok(Command::Get(key.unwrap())),
                                _ => Err(CommandError::InvalidCommand),
                            }
                        }
                        "SET" => {
                            if elements.len() < 3 || elements.len() > 4 {
                                return Err(CommandError::InvalidCommand);
                            }

                            let key = elements[1].clone();
                            let value = elements[2].clone();
                            let expiry = if elements.len() == 4 {
                                let expiry = elements[3].clone();
                                match expiry {
                                    RespElement::Integer(expiry) => Some(expiry),
                                    _ => return Err(CommandError::InvalidCommand),
                                }
                            } else {
                                None
                            };
                            match (key, value) {
                                (RespElement::BulkString(key), RespElement::BulkString(value)) => {
                                    Ok(Command::Set {
                                        key: key.unwrap(),
                                        value: value.unwrap(),
                                        expiry: expiry.map(|expiry| expiry as u64),
                                    })
                                }
                                _ => Err(CommandError::InvalidCommand),
                            }
                        }
                        _ => Err(CommandError::UnknownCommand),
                    },
                    _ => Err(CommandError::UnknownCommand),
                }
            }
            _ => Err(CommandError::UnknownCommand),
        }
    }
}
