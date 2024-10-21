use nom::branch::alt;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{crlf, i64 as i64_parser, u32 as u32_parser};
use nom::combinator::map;
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Bulk strings
///
/// A bulk string represents a single binary string.
/// The string can be of any size, but by default,
/// Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BulkString(pub String);

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], BulkString> {
    let (input, _) = tag(b"$")(input)?;
    let (input, len) = u32_parser(input)?;
    let (input, _) = crlf(input)?;
    let (s, input) = input.split_at(len.try_into().unwrap());
    let (input, _) = crlf(input)?;
    Ok((
        input,
        BulkString(std::str::from_utf8(s).unwrap().to_string()),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RespElement {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(Vec<RespElement>),
}

fn parse_element(input: &[u8]) -> IResult<&[u8], RespElement> {
    alt((
        map(parse_simple_string, RespElement::SimpleString),
        map(parse_simple_error, RespElement::SimpleError),
        map(parse_integer, RespElement::Integer),
        map(parse_bulk_string, RespElement::BulkString),
        map(parse_array, RespElement::Array),
    ))(input)
}

/// Arrays
///
/// Clients send commands to the Redis server as RESP arrays.
/// Similarly, some Redis commands that return collections of elements use arrays as their replies.
/// An example is the LRANGE command that returns elements of a list.
fn parse_array(input: &[u8]) -> IResult<&[u8], Vec<RespElement>> {
    let (input, _) = tag(b"*")(input)?;
    let (input, len) = u32_parser(input)?;
    let (input, _) = crlf(input)?;

    let mut rest = input;
    let mut elements = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (r, element) = parse_element(rest)?;
        elements.push(element);
        rest = r;
    }

    Ok((rest, elements))
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
    fn test_parse_simple_string<'a>(
        #[case] bytes: &'a [u8],
        #[case] expected: &'a str,
    ) -> TestResult<'a> {
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
    #[case(
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    )]
    fn test_parse_simple_error<'a>(
        #[case] bytes: &'a [u8],
        #[case] expected: &'a str,
    ) -> TestResult<'a> {
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
        let (rest, int) = parse_integer(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(int, expected);
        Ok(())
    }

    #[rstest]
    #[case(b"$5\r\nhello\r\n", "hello")]
    #[case(b"$0\r\n\r\n", "")]
    fn test_parse_bulk_string<'a>(
        #[case] bytes: &'a [u8],
        #[case] expected: &'a str,
    ) -> TestResult<'a> {
        let (rest, bs) = parse_bulk_string(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(&bs.0, expected);
        Ok(())
    }

    #[rstest]
    #[case(b"+Ok\r\n", RespElement::SimpleString(SimpleString("Ok".to_string())))]
    #[case(b"-ERR unknown command 'asdf'\r\n", RespElement::SimpleError(SimpleError("ERR unknown command 'asdf'".to_string())))]
    #[case(b":0\r\n", RespElement::Integer(0))]
    #[case(b"$5\r\nhello\r\n", RespElement::BulkString(BulkString("hello".to_string())))]
    fn test_parse_element<'a>(
        #[case] bytes: &'a [u8],
        #[case] expected: RespElement,
    ) -> TestResult<'a> {
        let (rest, element) = parse_element(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(element, expected);
        Ok(())
    }

    #[rstest]
    #[case(b"*0\r\n", vec![])]
    #[case(
        b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
        vec![
            RespElement::BulkString(BulkString("hello".to_string())),
            RespElement::BulkString(BulkString("world".to_string()))
        ]
    )]
    #[case(
        b"*3\r\n:1\r\n:2\r\n:3\r\n",
        vec![RespElement::Integer(1), RespElement::Integer(2), RespElement::Integer(3)]
    )]
    #[case(
        b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n",
        vec![
            RespElement::Integer(1),
            RespElement::Integer(2),
            RespElement::Integer(3),
            RespElement::Integer(4),
            RespElement::BulkString(BulkString("hello".to_string()))
        ]
    )]
    #[case(
        b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
        vec![
            RespElement::Array(vec![
                RespElement::Integer(1),
                RespElement::Integer(2),
                RespElement::Integer(3)
            ]),
            RespElement::Array(vec![
                RespElement::SimpleString(SimpleString("Hello".to_string())),
                RespElement::SimpleError(SimpleError("World".to_string()))
            ])
        ]
    )]
    fn test_parse_array<'a>(
        #[case] bytes: &'a [u8],
        #[case] expected: Vec<RespElement>,
    ) -> TestResult<'a> {
        let (rest, elements) = parse_array(bytes)?;
        assert_eq!(rest, b"");
        assert_eq!(elements, expected);
        Ok(())
    }
}
