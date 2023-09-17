use std::fmt;

use datafusion_sql::sqlparser::ast::{Expr, Fetch, LockClause, Offset, OrderByExpr, SetExpr, With};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PaimonQuery {
    /// WITH (common table expressions, or CTEs)
    pub with: Option<With>,
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: Box<SetExpr>,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> [ { ROW | ROWS } ]`
    pub offset: Option<Offset>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
    /// `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
    pub locks: Vec<LockClause>,
}

impl fmt::Display for PaimonQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        write!(f, "{}", self.body)?;
        // if !self.order_by.is_empty() {
        //     write!(f, " ORDER BY {}", display_comma_separated(&self.order_by))?;
        // }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " {offset}")?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " {fetch}")?;
        }
        // if !self.locks.is_empty() {
        //     write!(f, " {}", display_separated(&self.locks, " "))?;
        // }
        Ok(())
    }
}
