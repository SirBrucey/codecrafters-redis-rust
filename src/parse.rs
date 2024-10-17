use nom::bytes::complete::{tag, take_until1};
use nom::IResult;

static CR: &[u8] = b"\r";
static LF: &[u8] = b"\n";
static CRLF: &[u8] = b"\r\n";
#[derive(Debug, Clone)]
struct SimpleString(pub String);

/// Simple strings are encoded as a plus (+) character, followed by a string.
/// The string mustn't contain a CR (\r) or LF (\n) character
/// and is terminated by CRLF (i.e., \r\n).
fn parse_simple_string(input: &[u8]) -> IResult<&[u8], SimpleString> {
    let (input, _) = tag(b"+")(input)?;
    let (input, s) = take_until1(CR)(input)?;
    let (input, _) = tag(CRLF)(input)?;
    // FIXME: find nicer way of converting slice to string
    Ok((input, SimpleString(std::str::from_utf8(s).unwrap().to_string())))
}

mod tests {
    use super::*;
    #[test]
    fn test_parse_simple_string() {
        let (rest, simple_string) = parse_simple_string(b"+Ok\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(simple_string.0, "Ok".to_string());
    }
}