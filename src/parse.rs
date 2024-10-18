use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{crlf, i64 as i64_parser};
use nom::IResult;

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, s) = is_not("\r\n")(input)?;
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
    let (input, _) = crlf(input)?;
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
    let (input, _) = crlf(input)?;

    Ok((input, SimpleError(s)))
}

/// Integers
///
/// This type is a CRLF-terminated string that represents a signed, base-10, 64-bit integer.
fn parse_integer(input: &[u8]) -> IResult<&[u8], i64> {
    let (input, _) = tag(b":")(input)?;
    let (input, n) = i64_parser(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, n))
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

    #[test]
    fn test_parse_integer_1() {
        let (rest, int)  = parse_integer(b":0\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(int, 0);
    }

    #[test]
    fn test_parse_integer_2() {
        let (rest, int)  = parse_integer(b":1000\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(int, 1000);
    }

    #[test]
    fn test_parse_integer_3() {
        let (rest, int)  = parse_integer(b":-42\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(int, -42);
    }

    #[test]
    fn test_parse_integer_4() {
        let (rest, int)  = parse_integer(b":+42\r\n").unwrap();
        assert_eq!(rest, b"");
        assert_eq!(int, 42);
    }
}