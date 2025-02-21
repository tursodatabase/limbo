mod keywords;

use keywords::KEYWORDS;
use limbo_ext::{
    register_extension, ExtensionApi, ResultCode, VTabCursor, VTabModule, VTabModuleDerive, Value,
};

register_extension! {
    vtabs: { CompletionVTab }
}

macro_rules! try_option {
    ($expr:expr, $err:expr) => {
        match $expr {
            Some(val) => val,
            None => return $err,
        }
    };
}

#[derive(Debug, Default, PartialEq, Clone)]
enum CompletionPhase {
    #[default]
    FirstPhase = 0,
    Keywords = 1,
    Pragmas = 2,
    Functions = 3,
    Collations = 4,
    Indexes = 5,
    Triggers = 6,
    Databases = 7,
    Tables = 8, // Also VIEWs and TRIGGERs
    Columns = 9,
    Modules = 10,
    Eof = 11,
}

impl Into<i64> for CompletionPhase {
    fn into(self) -> i64 {
        use self::CompletionPhase::*;
        match self {
            FirstPhase => 0,
            Keywords => 1,
            Pragmas => 2,
            Functions => 3,
            Collations => 4,
            Indexes => 5,
            Triggers => 6,
            Databases => 7,
            Tables => 8,
            Columns => 9,
            Modules => 10,
            Eof => 11,
        }
    }
}

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive)]
struct CompletionVTab {}

impl VTabModule for CompletionVTab {
    type VCursor = CompletionCursor;
    const NAME: &'static str = "completion";

    fn connect(api: &ExtensionApi) -> ResultCode {
        // Create table schema
        let sql = "CREATE TABLE completion(
            candidate TEXT,
            prefix TEXT HIDDEN,
            wholeline TEXT HIDDEN,
            phase INT HIDDEN     
        )";
        api.declare_virtual_table(Self::NAME, sql)
    }

    fn open() -> Self::VCursor {
        CompletionCursor::default()
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Value {
        cursor.column(idx)
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        cursor.next()
    }

    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.eof()
    }

    fn filter(cursor: &mut Self::VCursor, arg_count: i32, args: &[Value]) -> ResultCode {
        todo!()
    }
}

/// The cursor for iterating over the generated sequence
#[derive(Debug, Default)]
struct CompletionCursor {
    line: String,
    prefix: String,
    curr_row: String,
    rowid: i64,
    phase: CompletionPhase,
    inter_phase_counter: usize,
    // stmt: Statement
    // conn: Connection
}

impl CompletionCursor {}

impl VTabCursor for CompletionCursor {
    type Error = ResultCode;

    fn next(&mut self) -> ResultCode {
        let mut curr_col = -1 as isize;
        self.rowid += 1;
        let mut next_phase = CompletionPhase::FirstPhase;

        while self.phase != CompletionPhase::Eof {
            match self.phase {
                CompletionPhase::Keywords => {
                    if self.inter_phase_counter >= KEYWORDS.len() {
                        self.curr_row.clear();
                        self.phase = CompletionPhase::Databases;
                    } else {
                        self.inter_phase_counter += 1;
                        self.curr_row.push_str(KEYWORDS[self.inter_phase_counter]);
                    }
                }
                CompletionPhase::Databases => {
                    // TODO implement this when
                    // self.stmt = self.conn.prepare("PRAGMA database_list")
                    curr_col = 1;
                    next_phase = CompletionPhase::Tables;
                }
                _ => (),
            }
        }
        ResultCode::OK
    }

    fn eof(&self) -> bool {
        self.phase == CompletionPhase::Eof
    }

    fn column(&self, idx: u32) -> Value {
        match idx {
            0 => Value::from_text(self.curr_row.clone()), // COMPLETION_COLUMN_CANDIDATE
            1 => Value::from_text(self.prefix.clone()),   // COMPLETION_COLUMN_PREFIX
            2 => Value::from_text(self.line.clone()),     // COMPLETION_COLUMN_WHOLELINE
            3 => Value::from_integer(self.phase.clone().into()), // COMPLETION_COLUMN_PHASE
            _ => Value::null(),
        }
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }
}

#[cfg(test)]
mod tests {}
