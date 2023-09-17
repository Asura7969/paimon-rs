pub mod query;

use std::fmt;

use datafusion_sql::sqlparser::{
    ast::{Offset, OffsetRows, Query, SqlOption, Statement, With},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

#[derive(Debug)]
pub struct PaimonDialect {}

impl Dialect for PaimonDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '$' || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_' || ch == '='
    }

    fn parse_statement(&self, parser: &mut Parser<'_>) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::SELECT) {
            parser.prev_token();
            Some(Ok(Statement::Query(Box::new(
                parse_query(parser).expect("sql parser error"),
            ))))
        } else {
            None
        }
    }
}

// pub fn parse_query(_parser: &mut Parser) -> Result<Statement, ParserError> {
//     todo!()
// }

pub fn parse_query(parser: &mut Parser<'_>) -> Result<Query, ParserError> {
    // let _guard = parser.recursion_counter.try_decrease()?;
    let with = if parser.parse_keyword(Keyword::WITH) {
        Some(With {
            recursive: parser.parse_keyword(Keyword::RECURSIVE),
            cte_tables: parser.parse_comma_separated(Parser::parse_cte)?,
        })
    } else {
        None
    };

    let body = Box::new(parser.parse_query_body(0)?);

    let order_by = if parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
        parser.parse_comma_separated(Parser::parse_order_by_expr)?
    } else {
        vec![]
    };

    let mut limit = None;
    let mut offset = None;

    for _x in 0..2 {
        if limit.is_none() && parser.parse_keyword(Keyword::LIMIT) {
            limit = parser.parse_limit()?
        }

        if offset.is_none() && parser.parse_keyword(Keyword::OFFSET) {
            offset = Some(parser.parse_offset()?)
        }

        if limit.is_some() && offset.is_none() && parser.consume_token(&Token::Comma) {
            // MySQL style LIMIT x,y => LIMIT y OFFSET x.
            // Check <https://dev.mysql.com/doc/refman/8.0/en/select.html> for more details.
            offset = Some(Offset {
                value: limit.unwrap(),
                rows: OffsetRows::None,
            });
            limit = Some(parser.parse_expr()?);
        }
    }

    let fetch = if parser.parse_keyword(Keyword::FETCH) {
        Some(parser.parse_fetch()?)
    } else {
        None
    };

    let mut locks = Vec::new();
    while parser.parse_keyword(Keyword::FOR) {
        locks.push(parser.parse_lock()?);
    }

    Ok(Query {
        with,
        body,
        order_by,
        limit,
        offset,
        fetch,
        locks,
    })
}

#[allow(dead_code)]
pub fn parse_options(parser: &mut Parser<'_>) -> Result<(), ParserError> {
    let (mut options, mut has_as, mut alias) = (vec![], false, None);

    if parser.peek_token().token != Token::EOF {
        if let Token::Word(word) = parser.peek_token().token {
            if word.keyword == Keyword::OPTIONS {
                options = parser
                    .parse_options(Keyword::OPTIONS)
                    .expect("options parser error");
            }
        };

        if parser.peek_token().token != Token::EOF {
            if let Token::Word(word) = parser.peek_token().token {
                if word.keyword == Keyword::AS {
                    has_as = true;
                    alias = Some(parser.parse_literal_string().expect("alias parser error"))
                }
            };
        }
    }

    println!(
        "has as: {}, alias: {:?}, options: {:?}",
        has_as, alias, options
    );

    Ok(())
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Options(Vec<SqlOption>);

impl fmt::Display for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OPTIONS ({:?})", self.0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    #[test]
    fn paimon_dialect_test() {
        // let sql = "select * from ods_mysql_paimon_points_5 OPTIONS('scan.snapshot-id' = '1')";
        // let dialect = PaimonDialect {};

        // let statements = Parser::parse_sql(&dialect, sql).unwrap();

        // println!("{:?}", statements);
    }
}
