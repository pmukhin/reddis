use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum CmdError {
    ParseError(String),
}

impl fmt::Display for CmdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CmdError::ParseError(message) => write!(f, "{}", message),
        }
    }
}

impl Error for CmdError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get(String),
    Set(String, Vec<u8>),
    SetEx(String, Vec<u8>, u64),
}

fn tokenize(s: String) -> Result<Vec<String>, CmdError> {
    if s.len() < 4 {
        return Result::Err(CmdError::ParseError("command too short".to_string()));
    }
    Ok(s.split(' ').map(|s| String::from(s)).collect::<Vec<_>>())
}

pub fn parse_command(s: String) -> Result<Command, CmdError> {
    let tokens = tokenize(s)?;

    if tokens[0] == "PING" {
        return Ok(Command::Ping);
    }
    if tokens[0] == "GET" && tokens.len() == 2 {
        return Ok(Command::Get(tokens[1].to_owned()));
    }
    if tokens[0] == "SET" && tokens.len() > 2 {
        let key = tokens[1].to_string();
        let data = tokens[2].as_bytes();
        return Result::Ok(Command::Set(key, data.to_vec()));
    }
    if tokens[0] == "SETEX" && tokens.len() == 4 {
        let key = &tokens[1];
        let ttl = &tokens[2];
        let data = tokens[3].as_bytes();

        return match ttl.parse::<u64>() {
            Ok(v) if v > 0 => Result::Ok(Command::SetEx(key.to_string(), data.to_vec(), v)),
            _ => Result::Err(CmdError::ParseError(
                "invalid expire time in 'setex' command".to_string(),
            )),
        };
    }

    Result::Err(CmdError::ParseError("invalid command".to_string()))
}

mod tests {
    #[test]
    fn test_parse_command_ping() {
        assert!(matches!(
            super::parse_command(String::from("PING")),
            Ok(super::Command::Ping)
        ))
    }
    #[test]
    fn test_parse_command_get() {
        assert!(matches!(
            super::parse_command(String::from("GET 21232")),
            Ok(super::Command::Get(_))
        ))
    }
    #[test]
    fn test_parse_command_set() {
        let result = super::parse_command(String::from("SET 21232 2412312"));

        assert!(matches!(result, Ok(super::Command::Set(_, _))))
    }
}
