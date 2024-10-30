use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{parse::RespElement, OptValue};

use super::{Command, CommandError, CommandExecutor, DbValue, FromResp};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct EchoCommand(String);

impl CommandExecutor for EchoCommand {
    fn execute(
        self,
        _db: &Arc<Mutex<HashMap<String, DbValue>>>,
        _opts: &Arc<HashMap<String, OptValue>>,
    ) -> RespElement {
        RespElement::BulkString(self.0.into())
    }
}

impl FromResp for EchoCommand {
    type Resp = Vec<RespElement>;

    fn from_resp(elements: Self::Resp) -> Result<Self, CommandError>
    where
        Self: Sized,
    {
        if elements.len() != 2 {
            return Err(CommandError::InvalidCommand);
        }

        if let RespElement::BulkString(command) = &elements[0] {
            if command.as_ref() != "ECHO" {
                return Err(CommandError::InvalidCommand);
            }
        }

        match &elements[1] {
            RespElement::BulkString(message) => Ok(EchoCommand(message.clone().unwrap())),
            _ => Err(CommandError::InvalidCommand),
        }
    }
}

impl From<EchoCommand> for Command {
    fn from(cmd: EchoCommand) -> Self {
        Self::Echo(cmd)
    }
}
