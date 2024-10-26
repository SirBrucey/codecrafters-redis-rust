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
    Set(SetCommand),
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
            Self::Set(set_cmd) => {
                let mut should_set = true;
                let mut db = db.lock().unwrap();
                if set_cmd.only_if.is_some() || set_cmd.get {
                    let exists = db.contains_key(&set_cmd.key);
                    match (exists, set_cmd.only_if) {
                        (true, Some(SetOnlyIf::DoesNotExists)) => should_set = false,
                        (false, Some(SetOnlyIf::AlreadyExists)) => should_set = false,
                        _ => {}
                    };
                };
                if should_set {
                    let old_value = db.insert(
                        set_cmd.key,
                        DbValue {
                            value: set_cmd.value.into(),
                            expires_at: if let Some(expiry) = set_cmd.expiry {
                                Some(match expiry {
                                    ExpiryOpt::Seconds(i) => {
                                        std::time::Instant::now()
                                            + std::time::Duration::from_secs(i)
                                    }
                                    ExpiryOpt::Milliseconds(i) => {
                                        std::time::Instant::now()
                                            + std::time::Duration::from_millis(i)
                                    }
                                    ExpiryOpt::TimestampSeconds(_) => todo!(),
                                    ExpiryOpt::TimestampMilliseconds(_) => todo!(),
                                    ExpiryOpt::KeepTtl => todo!(),
                                })
                            } else {
                                None
                            },
                        },
                    );

                    if set_cmd.get {
                        match old_value {
                            Some(db_value) => {
                                RespElement::BulkString(db_value.value.clone().into())
                            }
                            None => RespElement::Null,
                        }
                    } else {
                        RespElement::SimpleString("OK".to_owned().into())
                    }
                } else {
                    // NX or XX confilct.
                    RespElement::Null
                }
            }
        }
    }
}

pub(crate) enum CommandError {
    MissingCommand,
    InvalidCommand,
    UnknownCommand,
    SyntaxError,
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
                    RespElement::BulkString(command) => match command.as_ref() {
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
                            if elements.len() < 3 {
                                return Err(CommandError::InvalidCommand);
                            }

                            let key = elements[1].clone();
                            let key = match key {
                                RespElement::BulkString(key) => key.unwrap(),
                                _ => return Err(CommandError::InvalidCommand),
                            };
                            let value = elements[2].clone();
                            let value = match value {
                                RespElement::BulkString(value) => value.unwrap(),
                                _ => return Err(CommandError::InvalidCommand),
                            };

                            let mut only_if = None;
                            let mut get = false;
                            let mut expiry = None;

                            let mut idx = 3;
                            while idx < elements.len() {
                                let arg = &elements[idx];
                                match arg {
                                    RespElement::BulkString(arg) => match arg.as_ref() {
                                        "NX" if only_if.is_none() => {
                                            only_if = Some(SetOnlyIf::DoesNotExists);
                                            idx += 1;
                                        }
                                        "XX" if only_if.is_none() => {
                                            only_if = Some(SetOnlyIf::AlreadyExists);
                                            idx += 1;
                                        }
                                        "NX" | "XX" => return Err(CommandError::SyntaxError),
                                        "GET" if !get => {
                                            get = true;
                                            idx += 1;
                                        }
                                        "GET" => return Err(CommandError::SyntaxError),
                                        "EX" if expiry.is_none() => {
                                            let value = elements
                                                .get(idx + 1)
                                                .ok_or(CommandError::SyntaxError)?;
                                            if let RespElement::Integer(value) = value {
                                                expiry = Some(ExpiryOpt::Seconds(*value as u64));
                                                idx += 2;
                                            } else {
                                                return Err(CommandError::SyntaxError);
                                            }
                                        }
                                        "PX" if expiry.is_none() => {
                                            let value = elements
                                                .get(idx + 1)
                                                .ok_or(CommandError::SyntaxError)?;
                                            if let RespElement::Integer(value) = value {
                                                expiry =
                                                    Some(ExpiryOpt::Milliseconds(*value as u64));
                                                idx += 2;
                                            } else {
                                                return Err(CommandError::SyntaxError);
                                            }
                                        }
                                        "EXAT" if expiry.is_none() => {
                                            let value = elements
                                                .get(idx + 1)
                                                .ok_or(CommandError::SyntaxError)?;
                                            if let RespElement::Integer(value) = value {
                                                expiry = Some(ExpiryOpt::TimestampSeconds(
                                                    *value as u64,
                                                ));
                                                idx += 2;
                                            } else {
                                                return Err(CommandError::SyntaxError);
                                            }
                                        }
                                        "PXAT" if expiry.is_none() => {
                                            let value = elements
                                                .get(idx + 1)
                                                .ok_or(CommandError::SyntaxError)?;
                                            if let RespElement::Integer(value) = value {
                                                expiry = Some(ExpiryOpt::TimestampMilliseconds(
                                                    *value as u64,
                                                ));
                                                idx += 2;
                                            } else {
                                                return Err(CommandError::SyntaxError);
                                            }
                                        }
                                        "KEEPTTL" if expiry.is_none() => {
                                            expiry = Some(ExpiryOpt::KeepTtl);
                                            idx += 1;
                                        }
                                        "EX" | "PX" | "EXAT" | "PXAT" | "KEEPTTL" => {
                                            return Err(CommandError::SyntaxError)
                                        }
                                        _ => return Err(CommandError::InvalidCommand),
                                    },
                                    _ => return Err(CommandError::InvalidCommand),
                                }
                            }
                            Ok(Command::Set(SetCommand {
                                key,
                                value,
                                only_if,
                                get,
                                expiry,
                            }))
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

pub(crate) struct SetCommand {
    key: String,
    value: String,
    only_if: Option<SetOnlyIf>,
    get: bool,
    expiry: Option<ExpiryOpt>,
}

enum SetOnlyIf {
    DoesNotExists,
    AlreadyExists,
}

enum ExpiryOpt {
    Seconds(u64),
    Milliseconds(u64),
    TimestampSeconds(u64),
    TimestampMilliseconds(u64),
    KeepTtl,
}
