use super::ast::{Column, Expression};
use super::operators::peek_operator;
use super::tokenizer::{SqlTokenKind, SqlTokenStream};
use super::utils::{expect_token, SqlParseError};
use super::Operator;

pub(crate) fn parse_expr(
    input: &mut SqlTokenStream,
    min_precedence: u8,
) -> Result<Expression, SqlParseError> {
    let mut lhs = parse_atom(input)?;

    loop {
        let (op, precedence) = match peek_operator(input) {
            Some((op, prec)) if prec >= min_precedence => (op, prec),
            _ => break,
        };

        input.next_token(); // Consume the operator

        let rhs = parse_expr(input, precedence + 1)?;

        lhs = Expression::Binary {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        };
    }

    Ok(lhs)
}

fn parse_function_call(
    name: String,
    input: &mut SqlTokenStream,
) -> Result<Expression, SqlParseError> {
    // parse until we find a closing parenthesis. we expect to find Expr + comma, so we need to peek after
    // each Expr to see if we should continue parsing
    // at this point we have already consumed the opening parenthesis, so we can just parse the Expr
    if let Some(SqlTokenKind::ParenR) = input.peek_kind(0) {
        input.next_token();
        return Ok(Expression::FunctionCall { name, args: None });
    }
    let mut args = vec![parse_expr(input, 0)?];
    while let Some(SqlTokenKind::Comma) = input.peek_kind(0) {
        input.next_token();
        args.push(parse_expr(input, 0)?);
    }
    expect_token(input, SqlTokenKind::ParenR)?;
    Ok(Expression::FunctionCall {
        name,
        args: Some(args),
    })
}

fn parse_atom(input: &mut SqlTokenStream) -> Result<Expression, SqlParseError> {
    // if first token is an identifier, we need to check if it's a function call or a column
    if let Some(SqlTokenKind::Identifier) = input.peek_kind(0) {
        let id_token = input.next_token().unwrap();
        let name = std::str::from_utf8(id_token.materialize(&input.source)).unwrap();
        if let Some(SqlTokenKind::ParenL) = input.peek_kind(0) {
            // it's a function call
            input.next_token().unwrap();
            return parse_function_call(name.to_string(), input);
        } else {
            // it's a column. handle qualified column case
            let mut table_name = None;
            let column_name = name.to_string();

            // Check if the next token is a period, indicating a qualified column
            if let Some(SqlTokenKind::Period) = input.peek_kind(0) {
                input.next_token().unwrap();
                table_name = Some(column_name);

                // The column name should be the identifier after the dot
                if let Some(SqlTokenKind::Identifier) = input.peek_kind(0) {
                    let id_token = input.next_token().unwrap();
                    let col_name =
                        std::str::from_utf8(id_token.materialize(&input.source)).unwrap();
                    return Ok(Expression::Column(Column {
                        name: col_name.to_string(),
                        alias: None,
                        table_name,
                        table_no: None,
                        column_no: None,
                    }));
                } else if let Some(wrong_token) = input.peek(0) {
                    return Err(SqlParseError::new(format!(
                        "Expected column name after '.', got {}",
                        wrong_token.print(&input.source)
                    )));
                } else {
                    return Err(SqlParseError::new("Expected column name after '.'"));
                }
            }

            return Ok(Expression::Column(Column {
                name: column_name,
                alias: None,
                table_name,
                table_no: None,
                column_no: None,
            }));
        }
    }
    match input.peek_kind(0) {
        Some(SqlTokenKind::Literal) => {
            let literal_token = input.next_token().unwrap();
            let value = literal_token.materialize(&input.source);
            // try parsing bytes as string, then number, then blob
            let maybe_string = std::str::from_utf8(value);
            if let Ok(string) = maybe_string {
                let maybe_number = string.parse::<f64>();
                match maybe_number {
                    Ok(_) => return Ok(Expression::LiteralNumber(string.to_string())),
                    Err(_) => {
                        let leading_and_trailing_singlequote_byte_removed =
                            &string[1..string.len() - 1];
                        return Ok(Expression::LiteralString(
                            leading_and_trailing_singlequote_byte_removed.to_string(),
                        ));
                    }
                }
            }
            return Ok(Expression::LiteralBlob(value.to_vec()));
        }
        Some(SqlTokenKind::Asterisk) => {
            input.next_token().unwrap();
            Ok(Expression::LiteralString("*".to_string()))
        }
        Some(SqlTokenKind::Minus) => {
            input.next_token().unwrap();
            let expr = parse_expr(input, 0)?;
            Ok(Expression::Unary {
                op: Operator::Minus,
                expr: Box::new(expr),
            })
        }
        Some(SqlTokenKind::Like) => {
            // like is also a function call, so we need to parse it as such
            if let Some(SqlTokenKind::ParenL) = input.peek_kind(1) {
                input.next_token();
                input.next_token();
                return parse_function_call("like".to_string(), input);
            }

            Err(SqlParseError::new(
                "Expected '(' after 'like' when not used as a binary operator",
            ))
        }
        Some(SqlTokenKind::ParenL) => {
            input.next_token().unwrap();
            let expr = parse_expr(input, 0)?;
            expect_token(input, SqlTokenKind::ParenR)?;
            Ok(Expression::Parenthesized(Box::new(expr)))
        }
        Some(_) => {
            let wrong_token = input.peek(0).unwrap();
            Err(
                SqlParseError::new(format!(
                    "Expected one of: literal, identifier, function call, or parenthesized expression, got {}",
                    wrong_token.print(&input.source)
                ))
            )
        }
        None => Err(SqlParseError::new("Unexpected end of input")),
    }
}
