use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{parse::RespElement, OptValue};

use super::{CommandExecutor, DbValue};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct PingCommand;

impl CommandExecutor for PingCommand {
    fn execute(
        self,
        _db: &Arc<Mutex<HashMap<String, DbValue>>>,
        _opts: &Arc<HashMap<String, OptValue>>,
    ) -> RespElement {
        RespElement::SimpleString("PONG".to_owned().into())
    }
}
