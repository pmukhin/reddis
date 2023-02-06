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
    TtlCount,
    Get(String),
    Set(String, Vec<u8>),
    SetEx(String, Vec<u8>, u64),
    Lpush(String, Vec<Vec<u8>>, bool),
    Rpush(String, Vec<Vec<u8>>, bool),
    Lpop(String),
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
    if tokens[0] == "TTLCOUNT" {
        return Ok(Command::TtlCount);
    }
    if tokens[0] == "GET" && tokens.len() == 2 {
        return Ok(Command::Get(tokens[1].to_string()));
    }
    if tokens[0] == "SET" && tokens.len() == 3 {
        return Result::Ok(Command::Set(
            tokens[1].to_string(),
            tokens[2].as_bytes().to_vec(),
        ));
    }
    if tokens[0] == "SETEX" && tokens.len() == 4 {
        return match tokens[2].parse::<u64>() {
            Ok(v) if v > 0 => Result::Ok(Command::SetEx(
                tokens[1].to_string(),
                tokens[3].as_bytes().to_vec(),
                v,
            )),
            _ => Result::Err(CmdError::ParseError(
                "invalid expire time in 'setex' command".to_string(),
            )),
        };
    }

    fn drop_two(tokens: &Vec<String>) -> Vec<Vec<u8>> {
        tokens[2..]
            .iter()
            .map(|x| x.as_bytes().to_vec())
            .collect::<Vec<_>>()
    }

    if tokens[0] == "LPOP" && tokens.len() == 2 {
        return Result::Ok(Command::Lpop(tokens[1].to_string()));
    }

    if tokens[0] == "LPUSH" && tokens.len() > 2 {
        return Result::Ok(Command::Lpush(
            tokens[1].to_string(),
            drop_two(&tokens),
            true,
        ));
    }
    if tokens[0] == "RPUSH" && tokens.len() > 2 {
        return Result::Ok(Command::Rpush(
            tokens[1].to_string(),
            drop_two(&tokens),
            true,
        ));
    }
    if tokens[0] == "LPUSHX" && tokens.len() > 2 {
        return Result::Ok(Command::Lpush(
            tokens[1].to_string(),
            drop_two(&tokens),
            false,
        ));
    }
    if tokens[0] == "RPUSHX" && tokens.len() > 2 {
        return Result::Ok(Command::Rpush(
            tokens[1].to_string(),
            drop_two(&tokens),
            false,
        ));
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
