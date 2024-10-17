use nom::bytes::complete::{tag, take_while1};
use nom::IResult;

static CRLF: &[u8] = b"\r\n";

fn is_cr(chr: u8) -> bool {
    chr == b'\r'
}

fn is_lf(chr: u8) -> bool {
    chr == b'\n'
}

fn is_cr_or_lf(chr: u8) -> bool {
    is_cr(chr) || is_lf(chr)
}

fn not_cr_or_lf(chr: u8) -> bool {
    !is_cr_or_lf(chr)
}

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, s) = take_while1(not_cr_or_lf)(input)?;
    Ok((input, std::str::from_utf8(s).unwrap().to_string()))
}

/// Simple strings
///
/// Simple strings are encoded as a plus (+) character, followed by a string.
/// The string mustn't contain a CR (\r) or LF (\n) character
/// and is terminated by CRLF (i.e., \r\n).
#[derive(Debug, Clone)]
struct SimpleString(pub String);

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], SimpleString> {
    let (input, _) = tag(b"+")(input)?;
    let (input, s) = parse_string(input)?;
    let (input, _) = tag(CRLF)(input)?;
    Ok((input, SimpleString(s)))
}

/// Simple errors
//
// RESP has specific data types for errors.
// Simple errors, or simply just errors, are similar to simple strings,
// but their first character is the minus (-) character.
//
// The difference between simple strings and errors in RESP
// is that clients should treat errors as exceptions,
// whereas the string encoded in the error type is the error message itself.
#[derive(Debug, Clone)]
struct SimpleError(pub String);

fn parse_simple_error(input: &[u8]) -> IResult<&[u8], SimpleError> {
    let (input, _) = tag(b"-")(input)?;
    let (input, s) = parse_string(input)?;
    let (input, _) = tag(CRLF)(input)?;

    Ok((input, SimpleError(s)))
}

mod tests {
    use super::*;

    #[test]
    fn test_parse_string() {
        let (rest, s) = parse_string(b"Hello").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(s, "Hello".to_string());
    }

    #[test]
    fn test_parse_simple_string() {
        let (rest, simple_string) = parse_simple_string(b"+Ok\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(simple_string.0, "Ok".to_string());
    }

    #[test]
    fn test_parse_simple_error_1() {
        let (rest, simple_string) = parse_simple_error(b"-ERR unknown command 'asdf'\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(simple_string.0, "ERR unknown command 'asdf'".to_string());
    }

    #[test]
    fn test_parse_simple_error_2() {
        let (rest, simple_string) = parse_simple_error(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(simple_string.0, "WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
    }

}