use crate::parse::RespElement;

pub(crate) enum Command {
    Ping,
    Echo(String),
}

impl Command {
    pub(crate) fn execute(self) -> RespElement {
        match self {
            Self::Ping => RespElement::SimpleString("PONG".to_owned().into()),
            Self::Echo(message) => RespElement::BulkString(message.into()),
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
                        _ => Err(CommandError::UnknownCommand),
                    },
                    _ => Err(CommandError::UnknownCommand),
                }
            }
            _ => Err(CommandError::UnknownCommand),
        }
    }
}
