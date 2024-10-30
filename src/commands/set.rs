use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    parse::{NullBulkString, RespElement},
    OptValue,
};

use super::{parse_int, Command, CommandError, CommandExecutor, DbValue, FromResp};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SetCommand {
    key: String,
    value: String,
    only_if: Option<SetOnlyIf>,
    get: bool,
    expiry: Option<ExpiryOpt>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SetOnlyIf {
    DoesNotExists,
    AlreadyExists,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ExpiryOpt {
    Seconds(u64),
    Milliseconds(u64),
    TimestampSeconds(u64),
    TimestampMilliseconds(u64),
    KeepTtl,
}

impl CommandExecutor for SetCommand {
    fn execute(
        self,
        db: &Arc<Mutex<HashMap<String, DbValue>>>,
        _opts: &Arc<HashMap<String, OptValue>>,
    ) -> RespElement {
        let mut should_set = true;
        let mut db = db.lock().unwrap();
        if self.only_if.is_some() || self.get {
            let exists = db.contains_key(&self.key);
            match (exists, self.only_if) {
                (true, Some(SetOnlyIf::DoesNotExists)) => should_set = false,
                (false, Some(SetOnlyIf::AlreadyExists)) => should_set = false,
                _ => {}
            };
        };
        if should_set {
            let old_value = db.insert(
                self.key,
                DbValue {
                    value: self.value.into(),
                    expires_at: if let Some(expiry) = self.expiry {
                        Some(match expiry {
                            ExpiryOpt::Seconds(i) => {
                                std::time::Instant::now() + std::time::Duration::from_secs(i)
                            }
                            ExpiryOpt::Milliseconds(i) => {
                                std::time::Instant::now() + std::time::Duration::from_millis(i)
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

            if self.get {
                match old_value {
                    Some(db_value) => RespElement::BulkString(db_value.value.clone().into()),
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
}

impl FromResp for SetCommand {
    type Resp = Vec<RespElement>;

    fn from_resp(elements: Self::Resp) -> Result<Self, super::CommandError>
    where
        Self: Sized,
    {
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
                            let value = elements.get(idx + 1).ok_or(CommandError::SyntaxError)?;
                            expiry = Some(ExpiryOpt::Seconds(parse_int(value)?));
                            idx += 2;
                        }
                        "PX" if expiry.is_none() => {
                            let value = elements.get(idx + 1).ok_or(CommandError::SyntaxError)?;
                            expiry = Some(ExpiryOpt::Milliseconds(parse_int(value)?));
                            idx += 2;
                        }
                        "EXAT" if expiry.is_none() => {
                            let value = elements.get(idx + 1).ok_or(CommandError::SyntaxError)?;
                            expiry = Some(ExpiryOpt::TimestampSeconds(parse_int(value)?));
                            idx += 2;
                        }
                        "PXAT" if expiry.is_none() => {
                            let value = elements.get(idx + 1).ok_or(CommandError::SyntaxError)?;
                            expiry = Some(ExpiryOpt::TimestampMilliseconds(parse_int(value)?));
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
        Ok(SetCommand {
            key,
            value,
            only_if,
            get,
            expiry,
        })
    }
}

impl From<SetCommand> for Command {
    fn from(cmd: SetCommand) -> Self {
        Command::Set(cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_set_command() {
        let command = Command::try_from(RespElement::Array(vec![
            RespElement::BulkString("SET".to_owned().into()),
            RespElement::BulkString("key".to_owned().into()),
            RespElement::BulkString("value".to_owned().into()),
        ]))
        .unwrap();

        if let Command::Set(set_command) = command {
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
        let command = Command::try_from(RespElement::Array(vec![
            RespElement::BulkString("SET".to_owned().into()),
            RespElement::BulkString("key".to_owned().into()),
            RespElement::BulkString("value".to_owned().into()),
            RespElement::BulkString("PX".to_owned().into()),
            RespElement::Integer(1000),
        ]))
        .unwrap();

        if let Command::Set(set_command) = command {
            assert_eq!(set_command.key, "key");
            assert_eq!(set_command.value, "value");
            assert_eq!(set_command.only_if, None);
            assert_eq!(set_command.get, false);
            assert_eq!(set_command.expiry, Some(ExpiryOpt::Milliseconds(1000)));
        } else {
            panic!("Expected SET command");
        }
    }

    #[test]
    fn test_set_command_with_px_as_bulk_string() {
        let command = Command::try_from(RespElement::Array(vec![
            RespElement::BulkString("SET".to_owned().into()),
            RespElement::BulkString("key".to_owned().into()),
            RespElement::BulkString("value".to_owned().into()),
            RespElement::BulkString("PX".to_owned().into()),
            RespElement::BulkString("1000".to_owned().into()),
        ]))
        .unwrap();

        if let Command::Set(set_command) = command {
            assert_eq!(set_command.key, "key");
            assert_eq!(set_command.value, "value");
            assert_eq!(set_command.only_if, None);
            assert_eq!(set_command.get, false);
            assert_eq!(set_command.expiry, Some(ExpiryOpt::Milliseconds(1000)));
        } else {
            panic!("Expected SET command");
        }
    }

    #[test]
    fn test_execute_set_command_with_expiry() {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let command = Command::Set(SetCommand {
            key: "key".to_owned(),
            value: "value".to_owned(),
            only_if: None,
            get: false,
            expiry: Some(ExpiryOpt::Seconds(1)),
        });
        let resp = command.execute(&db, &Arc::new(HashMap::new()));
        assert_eq!(db.lock().unwrap().get("key").unwrap().value, "value");
        assert_eq!(resp, RespElement::SimpleString("OK".to_owned().into()));
    }
}
