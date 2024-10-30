use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

pub(crate) mod echo;
pub(crate) mod ping;
pub(crate) mod set;

use {echo::*, ping::*, set::*};

use crate::{
    parse::{NullBulkString, RespElement},
    OptValue,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum Command {
    Ping(PingCommand),
    Echo(EchoCommand),
    Get(String),
    Set(SetCommand),
    GetConfig(Vec<String>),
}

trait CommandExecutor {
    fn execute(
        self,
        db: &Arc<Mutex<HashMap<String, DbValue>>>,
        opts: &Arc<HashMap<String, OptValue>>,
    ) -> RespElement;
}

trait FromResp {
    type Resp;

    fn from_resp(element: Self::Resp) -> Result<Self, CommandError>
    where
        Self: Sized;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DbValue {
    value: Bytes,
    expires_at: Option<std::time::Instant>,
}

impl Command {
    pub(crate) fn execute(
        self,
        db: &Arc<Mutex<HashMap<String, DbValue>>>,
        opts: &Arc<HashMap<String, OptValue>>,
    ) -> RespElement {
        match self {
            Self::Ping(ping_cmd) => ping_cmd.execute(db, opts),
            Self::Echo(echo_cmd) => echo_cmd.execute(db, opts),
            Self::Get(key) => {
                let db = db.lock().unwrap();
                match db.get(&key) {
                    Some(db_value) => {
                        if let Some(expires_at) = db_value.expires_at {
                            if expires_at < std::time::Instant::now() {
                                return NullBulkString.into();
                            }
                        }

                        RespElement::BulkString(db_value.value.clone().into())
                    }
                    None => NullBulkString.into(),
                }
            }
            Self::Set(set_cmd) => set_cmd.execute(db, opts),
            Self::GetConfig(params) => {
                let mut vec = Vec::with_capacity(params.len());
                for param in params {
                    if let Some(value) = opts.get(&param) {
                        vec.push(RespElement::BulkString(param.into()));
                        vec.push(value.into());
                    }
                }
                RespElement::Array(vec)
            }
        }
    }
}

// FIXME: These clones do not feel good.
impl From<&OptValue> for RespElement {
    fn from(value: &OptValue) -> Self {
        match value {
            OptValue::String(s) => RespElement::BulkString(s.clone().into()),
            OptValue::UInt(i) => RespElement::Integer(*i as i64),
            OptValue::Path(path_buf) => RespElement::BulkString(
                path_buf
                    .clone()
                    .into_os_string()
                    .into_string()
                    .unwrap()
                    .into(),
            ),
        }
    }
}

#[derive(Debug)]
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
                        "PING" => Ok(Command::Ping(PingCommand)),
                        "ECHO" => Ok(EchoCommand::from_resp(elements)?.into()),
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
                        "SET" => Ok(SetCommand::from_resp(elements)?.into()),
                        "CONFIG" => {
                            let subcommand = elements.get(1).ok_or(CommandError::SyntaxError)?;
                            let subcommand = match subcommand {
                                RespElement::BulkString(subcommand) => subcommand.as_ref(),
                                _ => return Err(CommandError::SyntaxError),
                            };
                            match subcommand {
                                "GET" => {
                                    let mut params = Vec::with_capacity(elements.len() - 2);
                                    for i in 2..elements.len() {
                                        params.push(match &elements[i] {
                                            RespElement::BulkString(param) => {
                                                param.as_ref().to_owned()
                                            }
                                            _ => return Err(CommandError::SyntaxError),
                                        });
                                    }
                                    Ok(Command::GetConfig(params))
                                }
                                _ => Err(CommandError::UnknownCommand),
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

// Docs say expiries should be positive integers, but the tests were sending a bulk string.
fn parse_int(element: &RespElement) -> Result<u64, CommandError> {
    match element {
        RespElement::Integer(value) => Ok(*value as u64),
        RespElement::BulkString(value) => Ok(value
            .as_ref()
            .parse()
            .map_err(|_| CommandError::SyntaxError)?),
        _ => Err(CommandError::SyntaxError),
    }
}
