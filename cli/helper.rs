use std::rc::Rc;
use std::sync::Arc;

use limbo_core::{Connection, StepResult};
use rustyline::completion::{extract_word, Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::HistoryHinter;
use rustyline::{Completer, Helper, Hinter, Validator};
use syntect::dumps::from_uncompressed_data;
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::{Scope, SyntaxSet};
use syntect::util::{as_24_bit_terminal_escaped, LinesWithEndings};

macro_rules! try_result {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => return Ok($err),
        }
    };
}

#[derive(Helper, Completer, Hinter, Validator)]
pub struct LimboHelper {
    #[rustyline(Completer)]
    completer: SqlCompleter,
    syntax_set: SyntaxSet,
    theme_set: ThemeSet,
    #[rustyline(Hinter)]
    hinter: HistoryHinter,
}

impl LimboHelper {
    pub fn new(conn: Rc<Connection>, io: Arc<dyn limbo_core::IO>) -> Self {
        // Load only predefined syntax
        let ps = from_uncompressed_data(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/SQL_syntax_set_dump.packdump"
        )))
        .unwrap();
        let ts = ThemeSet::load_defaults();
        LimboHelper {
            completer: SqlCompleter::new(conn, io),
            syntax_set: ps,
            theme_set: ts,
            hinter: HistoryHinter::new(),
        }
    }
}

impl Highlighter for LimboHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> std::borrow::Cow<'l, str> {
        let _ = pos;
        // TODO use lifetimes to store highlight lines
        let syntax = self
            .syntax_set
            .find_syntax_by_scope(Scope::new("source.sql").unwrap())
            .unwrap();
        let mut h = HighlightLines::new(syntax, &self.theme_set.themes["base16-ocean.dark"]);
        let mut ret_line = String::new();
        for new_line in LinesWithEndings::from(line) {
            let ranges: Vec<(Style, &str)> = h.highlight_line(new_line, &self.syntax_set).unwrap();
            let escaped = as_24_bit_terminal_escaped(&ranges[..], false);
            ret_line.push_str(&escaped);
        }
        // Push this escape sequence to reset
        ret_line.push_str("\x1b[0m");
        std::borrow::Cow::Owned(ret_line)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> std::borrow::Cow<'b, str> {
        let _ = default;
        // Make prompt bold
        std::borrow::Cow::Owned(format!("\x1b[1;32m{}\x1b[0m", prompt))
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Owned(format!("\x1b[1;2;4;246m{hint}\x1b[0m"))
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        completion: rustyline::CompletionType,
    ) -> std::borrow::Cow<'c, str> {
        let _ = completion;
        std::borrow::Cow::Borrowed(candidate)
    }

    fn highlight_char(&self, line: &str, pos: usize, kind: rustyline::highlight::CmdKind) -> bool {
        let _ = (line, pos);
        !matches!(kind, rustyline::highlight::CmdKind::MoveCursor)
    }
}

pub struct SqlCompleter {
    conn: Rc<Connection>,
    io: Arc<dyn limbo_core::IO>,
}

impl SqlCompleter {
    pub fn new(conn: Rc<Connection>, io: Arc<dyn limbo_core::IO>) -> Self {
        Self { conn, io }
    }
}

// Got this from the FilenameCompleter.
// TODO have to see what chars break words in Sqlite
cfg_if::cfg_if! {
    if #[cfg(unix)] {
        // rl_basic_word_break_characters, rl_completer_word_break_characters
        const fn default_break_chars(c : char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\\' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' |
            '{' | '(' | '\0')
        }
        const ESCAPE_CHAR: Option<char> = Some('\\');
        // In double quotes, not all break_chars need to be escaped
        // https://www.gnu.org/software/bash/manual/html_node/Double-Quotes.html
        #[allow(dead_code)]
        const fn double_quotes_special_chars(c: char) -> bool { matches!(c, '"' | '$' | '\\' | '`') }
    } else if #[cfg(windows)] {
        // Remove \ to make file completion works on windows
        const fn default_break_chars(c: char) -> bool {
            matches!(c, ' ' | '\t' | '\n' | '"' | '\'' | '`' | '@' | '$' | '>' | '<' | '=' | ';' | '|' | '&' | '{' |
            '(' | '\0')
        }
        const ESCAPE_CHAR: Option<char> = None;
        #[allow(dead_code)]
        const fn double_quotes_special_chars(c: char) -> bool { c == '"' } // TODO Validate: only '"' ?
    } else if #[cfg(target_arch = "wasm32")] {
        const fn default_break_chars(c: char) -> bool { false }
        const ESCAPE_CHAR: Option<char> = None;
        #[allow(dead_code)]
        const fn double_quotes_special_chars(c: char) -> bool { false }
    }
}

impl Completer for SqlCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        // TODO: have to differentiate words if they are enclosed in single of double quotes
        let (prefix_pos, prefix) = extract_word(line, pos, ESCAPE_CHAR, default_break_chars);
        let mut candidates = Vec::new();

        let query = try_result!(
            self.conn.query(format!(
                "SELECT candidate FROM completion('{prefix}', '{line}') ORDER BY 1;"
            )),
            (prefix_pos, candidates)
        );

        if let Some(mut rows) = query {
            loop {
                match try_result!(rows.step(), (prefix_pos, candidates)) {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let completion: &str =
                            try_result!(row.get::<&str>(0), (prefix_pos, candidates));
                        let pair = Pair {
                            display: completion.to_string(),
                            replacement: completion.to_string(),
                        };
                        candidates.push(pair);
                    }
                    StepResult::IO => {
                        try_result!(self.io.run_once(), (prefix_pos, candidates));
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        break;
                    }
                }
            }
        }

        Ok((prefix_pos, candidates))
    }
}
