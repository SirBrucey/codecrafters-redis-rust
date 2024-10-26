use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use crate::{
    parse::{NullBulkString, RespElement},
    OptValue,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(SetCommand),
    GetConfig(Vec<String>),
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
            Self::Ping => RespElement::SimpleString("PONG".to_owned().into()),
            Self::Echo(message) => RespElement::BulkString(message.into()),
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
                            None => NullBulkString.into(),
                        }
                    } else {
                        RespElement::SimpleString("OK".to_owned().into())
                    }
                } else {
                    // NX or XX confilct.
                    NullBulkString.into()
                }
            }
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
                                    RespElement::BulkString(arg) => {
                                        let arg: &str = arg.as_ref();
                                        let arg = arg.to_uppercase();

                                        match arg.as_str() {
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
                                                expiry =
                                                    Some(ExpiryOpt::Seconds(parse_int(value)?));
                                                idx += 2;
                                            }
                                            "PX" if expiry.is_none() => {
                                                let value = elements
                                                    .get(idx + 1)
                                                    .ok_or(CommandError::SyntaxError)?;
                                                expiry = Some(ExpiryOpt::Milliseconds(parse_int(
                                                    value,
                                                )?));
                                                idx += 2;
                                            }
                                            "EXAT" if expiry.is_none() => {
                                                let value = elements
                                                    .get(idx + 1)
                                                    .ok_or(CommandError::SyntaxError)?;
                                                expiry = Some(ExpiryOpt::TimestampSeconds(
                                                    parse_int(value)?,
                                                ));
                                                idx += 2;
                                            }
                                            "PXAT" if expiry.is_none() => {
                                                let value = elements
                                                    .get(idx + 1)
                                                    .ok_or(CommandError::SyntaxError)?;
                                                expiry = Some(ExpiryOpt::TimestampMilliseconds(
                                                    parse_int(value)?,
                                                ));
                                                idx += 2;
                                            }
                                            "KEEPTTL" if expiry.is_none() => {
                                                expiry = Some(ExpiryOpt::KeepTtl);
                                                idx += 1;
                                            }
                                            "EX" | "PX" | "EXAT" | "PXAT" | "KEEPTTL" => {
                                                return Err(CommandError::SyntaxError)
                                            }
                                            _ => return Err(CommandError::InvalidCommand),
                                        }
                                    }
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SetCommand {
    key: String,
    value: String,
    only_if: Option<SetOnlyIf>,
    get: bool,
    expiry: Option<ExpiryOpt>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SetOnlyIf {
    DoesNotExists,
    AlreadyExists,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ExpiryOpt {
    Seconds(u64),
    Milliseconds(u64),
    TimestampSeconds(u64),
    TimestampMilliseconds(u64),
    KeepTtl,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_basic_set_command() {
        let command = crate::command::Command::try_from(crate::parse::RespElement::Array(vec![
            crate::parse::RespElement::BulkString("SET".to_owned().into()),
            crate::parse::RespElement::BulkString("key".to_owned().into()),
            crate::parse::RespElement::BulkString("value".to_owned().into()),
        ]))
        .unwrap();

        if let crate::command::Command::Set(set_command) = command {
            assert_eq!(set_command.key, "key");
            assert_eq!(set_command.value, "value");
            assert_eq!(set_command.only_if, None);
            assert_eq!(set_command.get, false);
            assert_eq!(set_command.expiry, None);
        } else {
            panic!("Expected SET command");
        }
    }

    #[test]
    fn test_set_command_with_px() {
        let command = crate::command::Command::try_from(crate::parse::RespElement::Array(vec![
            crate::parse::RespElement::BulkString("SET".to_owned().into()),
            crate::parse::RespElement::BulkString("key".to_owned().into()),
            crate::parse::RespElement::BulkString("value".to_owned().into()),
            crate::parse::RespElement::BulkString("PX".to_owned().into()),
            crate::parse::RespElement::Integer(1000),
        ]))
        .unwrap();

        if let crate::command::Command::Set(set_command) = command {
            assert_eq!(set_command.key, "key");
            assert_eq!(set_command.value, "value");
            assert_eq!(set_command.only_if, None);
            assert_eq!(set_command.get, false);
            assert_eq!(
                set_command.expiry,
                Some(crate::command::ExpiryOpt::Milliseconds(1000))
            );
        } else {
            panic!("Expected SET command");
        }
    }

    #[test]
    fn test_set_command_with_px_as_bulk_string() {
        let command = crate::command::Command::try_from(crate::parse::RespElement::Array(vec![
            crate::parse::RespElement::BulkString("SET".to_owned().into()),
            crate::parse::RespElement::BulkString("key".to_owned().into()),
            crate::parse::RespElement::BulkString("value".to_owned().into()),
            crate::parse::RespElement::BulkString("PX".to_owned().into()),
            crate::parse::RespElement::BulkString("1000".to_owned().into()),
        ]))
        .unwrap();

        if let crate::command::Command::Set(set_command) = command {
            assert_eq!(set_command.key, "key");
            assert_eq!(set_command.value, "value");
            assert_eq!(set_command.only_if, None);
            assert_eq!(set_command.get, false);
            assert_eq!(
                set_command.expiry,
                Some(crate::command::ExpiryOpt::Milliseconds(1000))
            );
        } else {
            panic!("Expected SET command");
        }
    }

    #[test]
    fn test_execute_set_command_with_expiry() {
        let db = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let command = crate::command::Command::Set(crate::command::SetCommand {
            key: "key".to_owned(),
            value: "value".to_owned(),
            only_if: None,
            get: false,
            expiry: Some(crate::command::ExpiryOpt::Seconds(1)),
        });
        let resp = command.execute(&db);
        assert_eq!(db.lock().unwrap().get("key").unwrap().value, "value");
        assert_eq!(
            resp,
            crate::parse::RespElement::SimpleString("OK".to_owned().into())
        );
    }
}
