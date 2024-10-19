use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{crlf, i64 as i64_parser};
use nom::IResult;

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    if input.is_empty() || input == b"\r\n" {
        Ok((input, "".to_string()))
    } else {
        let (input, s) = is_not("\r\n")(input)?;
        Ok((input, std::str::from_utf8(s).unwrap().to_string()))
    }
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
    use rstest::rstest;

    type TestResult<'a> = Result<(), nom::Err<nom::error::Error<&'a [u8]>>>;

    #[rstest]
    #[case(b"Hello", "Hello")]
    #[case(b"", "")]
    fn test_parse_string<'a>(#[case] bytes: &'a [u8], #[case] expected: &'a str) -> TestResult<'a> {
        let (rest, s) = parse_string(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(&s, expected);
        Ok(())
    }

    #[rstest]
    #[case(b"+Ok\r\n", "Ok")]
    #[case(b"+\r\n", "")]
    fn test_parse_simple_string<'a>(#[case] bytes: &'a [u8], #[case] expected: &'a str) -> TestResult<'a> {
        let (rest, simple_string) = parse_simple_string(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(&simple_string.0, expected);
        Ok(())
    }

    #[rstest]
    #[case(b"+O\rnk\r\n")]
    #[case(b"+O\nnk\r\n")]
    #[case(b"+Ok\r")]
    #[case(b"+Ok\n")]
    fn test_parse_simple_string_incorrect_termination(#[case] bytes: &[u8]) {
        assert!(parse_simple_string(bytes).is_err());
    }

    #[rstest]
    #[case(b"-ERR unknown command 'asdf'\r\n", "ERR unknown command 'asdf'")]
    #[case(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", "WRONGTYPE Operation against a key holding the wrong kind of value")]
    fn test_parse_simple_error<'a>(#[case] bytes: &'a [u8], #[case] expected: &'a str) -> TestResult<'a> {
        let (rest, simple_err) = parse_simple_error(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(&simple_err.0, expected);
        Ok(())
    }

    #[rstest]
    #[case(b":0\r\n", 0)]
    #[case(b":1000\r\n", 1000)]
    #[case(b":-42\r\n", -42)]
    #[case(b":+42\r\n", 42)]
    fn test_parse_integer<'a>(#[case] bytes: &'a [u8], #[case] expected: i64) -> TestResult<'a> {
        let (rest, int)  = parse_integer(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(int, expected);
        Ok(())
    }
}