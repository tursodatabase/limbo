use std::{
    cell::Cell,
    cmp::Ordering,
    collections::HashMap,
    rc::{Rc, Weak},
    sync::Arc,
};

use limbo_sqlite3_parser::ast;

use crate::{
    fast_lock::SpinLock,
    parameters::Parameters,
    schema::{BTreeTable, Index, PseudoTable},
    storage::sqlite3_ondisk::DatabaseHeader,
    translate::plan::{ResultSetColumn, TableReference},
    Connection, VirtualTable,
};

use super::{BranchOffset, CursorID, Insn, InsnFunction, InsnReference, JumpTarget, Program};
#[allow(dead_code)]
pub struct ProgramBuilder {
    next_free_register: usize,
    next_free_cursor_id: usize,
    /// Instruction, the function to execute it with, and its original index in the vector.
    insns: Vec<(Insn, InsnFunction, usize)>,
    pub constant_spans: Vec<(usize, usize)>,
    // Vector of labels which must be assigned to next emitted instruction
    next_insn_labels: Vec<BranchOffset>,
    // Cursors that are referenced by the program. Indexed by CursorID.
    pub cursor_ref: Vec<(Option<String>, CursorType)>,
    /// A vector where index=label number, value=resolved offset. Resolved in build().
    label_to_resolved_offset: Vec<Option<(InsnReference, JumpTarget)>>,
    // Bitmask of cursors that have emitted a SeekRowid instruction.
    seekrowid_emitted_bitmask: u64,
    // map of instruction index to manual comment (used in EXPLAIN only)
    comments: Option<HashMap<InsnReference, &'static str>>,
    pub parameters: Parameters,
    pub result_columns: Vec<ResultSetColumn>,
    pub table_references: Vec<TableReference>,
}

#[derive(Debug, Clone)]
pub enum CursorType {
    BTreeTable(Rc<BTreeTable>),
    BTreeIndex(Arc<Index>),
    Pseudo(Rc<PseudoTable>),
    Sorter,
    VirtualTable(Rc<VirtualTable>),
}

impl CursorType {
    pub fn is_index(&self) -> bool {
        matches!(self, CursorType::BTreeIndex(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum QueryMode {
    Normal,
    Explain,
}

impl From<ast::Cmd> for QueryMode {
    fn from(stmt: ast::Cmd) -> Self {
        match stmt {
            ast::Cmd::ExplainQueryPlan(_) | ast::Cmd::Explain(_) => QueryMode::Explain,
            _ => QueryMode::Normal,
        }
    }
}

pub struct ProgramBuilderOpts {
    pub query_mode: QueryMode,
    pub num_cursors: usize,
    pub approx_num_insns: usize,
    pub approx_num_labels: usize,
}

impl ProgramBuilder {
    pub fn new(opts: ProgramBuilderOpts) -> Self {
        Self {
            next_free_register: 1,
            next_free_cursor_id: 0,
            insns: Vec::with_capacity(opts.approx_num_insns),
            next_insn_labels: Vec::with_capacity(2),
            cursor_ref: Vec::with_capacity(opts.num_cursors),
            constant_spans: Vec::new(),
            label_to_resolved_offset: Vec::with_capacity(opts.approx_num_labels),
            seekrowid_emitted_bitmask: 0,
            comments: if opts.query_mode == QueryMode::Explain {
                Some(HashMap::new())
            } else {
                None
            },
            parameters: Parameters::new(),
            result_columns: Vec::new(),
            table_references: Vec::new(),
        }
    }

    pub fn constant_span_start(&mut self) -> usize {
        let span = self.constant_spans.len();
        let start = self.insns.len();
        self.constant_spans.push((start, usize::MAX));
        span
    }

    pub fn constant_span_end(&mut self, span_idx: usize) {
        let span = &mut self.constant_spans[span_idx];
        if span.1 == usize::MAX {
            span.1 = self.insns.len().saturating_sub(1);
        }
    }

    pub fn constant_span_end_all(&mut self) {
        for span in self.constant_spans.iter_mut() {
            if span.1 == usize::MAX {
                span.1 = self.insns.len().saturating_sub(1);
            }
        }
    }

    pub fn constant_span_is_open(&self) -> bool {
        self.constant_spans
            .last()
            .map_or(false, |(_, end)| *end == usize::MAX)
    }

    pub fn constant_spans_next_idx(&self) -> usize {
        self.constant_spans.len()
    }

    pub fn constant_spans_invalidate_after(&mut self, idx: usize) {
        self.constant_spans.truncate(idx);
    }

    pub fn alloc_register(&mut self) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += 1;
        reg
    }

    pub fn alloc_registers(&mut self, amount: usize) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += amount;
        reg
    }

    pub fn alloc_cursor_id(
        &mut self,
        table_identifier: Option<String>,
        cursor_type: CursorType,
    ) -> usize {
        let cursor = self.next_free_cursor_id;
        self.next_free_cursor_id += 1;
        self.cursor_ref.push((table_identifier, cursor_type));
        assert_eq!(self.cursor_ref.len(), self.next_free_cursor_id);
        cursor
    }

    pub fn emit_insn(&mut self, insn: Insn) {
        let function = insn.to_function();
        self.insns.push((insn, function, self.insns.len()));
    }

    pub fn close_cursors(&mut self, cursors: &[CursorID]) {
        for cursor in cursors {
            self.emit_insn(Insn::Close { cursor_id: *cursor });
        }
    }

    pub fn emit_string8(&mut self, value: String, dest: usize) {
        self.emit_insn(Insn::String8 { value, dest });
    }

    pub fn emit_string8_new_reg(&mut self, value: String) -> usize {
        let dest = self.alloc_register();
        self.emit_insn(Insn::String8 { value, dest });
        dest
    }

    pub fn emit_int(&mut self, value: i64, dest: usize) {
        self.emit_insn(Insn::Integer { value, dest });
    }

    pub fn emit_bool(&mut self, value: bool, dest: usize) {
        self.emit_insn(Insn::Integer {
            value: if value { 1 } else { 0 },
            dest,
        });
    }

    pub fn emit_null(&mut self, dest: usize, dest_end: Option<usize>) {
        self.emit_insn(Insn::Null { dest, dest_end });
    }

    pub fn emit_result_row(&mut self, start_reg: usize, count: usize) {
        self.emit_insn(Insn::ResultRow { start_reg, count });
    }

    pub fn emit_halt(&mut self) {
        self.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
        });
    }

    // no users yet, but I want to avoid someone else in the future
    // just adding parameters to emit_halt! If you use this, remove the
    // clippy warning please.
    #[allow(dead_code)]
    pub fn emit_halt_err(&mut self, err_code: usize, description: String) {
        self.emit_insn(Insn::Halt {
            err_code,
            description,
        });
    }

    pub fn emit_init(&mut self) -> BranchOffset {
        let target_pc = self.allocate_label();
        self.emit_insn(Insn::Init { target_pc });
        target_pc
    }

    pub fn emit_transaction(&mut self, write: bool) {
        self.emit_insn(Insn::Transaction { write });
    }

    pub fn emit_goto(&mut self, target_pc: BranchOffset) {
        self.emit_insn(Insn::Goto { target_pc });
    }

    pub fn add_comment(&mut self, insn_index: BranchOffset, comment: &'static str) {
        if let Some(comments) = &mut self.comments {
            comments.insert(insn_index.to_offset_int(), comment);
        }
    }

    pub fn mark_last_insn_constant(&mut self) {
        if self.constant_span_is_open() {
            // no need to mark this insn as constant as the surrounding parent expression is already constant
            return;
        }

        let prev = self.insns.len().saturating_sub(1);
        self.constant_spans.push((prev, prev));
    }

    pub fn emit_constant_insns(&mut self) {
        // move compile-time constant instructions to the end of the program, where they are executed once after Init jumps to it.
        // any label_to_resolved_offset that points to an instruction within any moved constant span should be updated to point to the new location.

        // the instruction reordering can be done by sorting the insns, so that the ordering is:
        // 1. if insn not in any constant span, it stays where it is
        // 2. if insn is in a constant span, it is after other insns, except those that are in a later constant span
        // 3. within a single constant span the order is preserver
        self.insns.sort_by(|(_, _, index_a), (_, _, index_b)| {
            let a_span = self
                .constant_spans
                .iter()
                .find(|span| span.0 <= *index_a && span.1 >= *index_a);
            let b_span = self
                .constant_spans
                .iter()
                .find(|span| span.0 <= *index_b && span.1 >= *index_b);
            if a_span.is_some() && b_span.is_some() {
                a_span.unwrap().0.cmp(&b_span.unwrap().0)
            } else if a_span.is_some() {
                Ordering::Greater
            } else if b_span.is_some() {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });
        for resolved_offset in self.label_to_resolved_offset.iter_mut() {
            if let Some((old_offset, target)) = resolved_offset {
                let new_offset = self
                    .insns
                    .iter()
                    .position(|(_, _, index)| *old_offset == *index as u32)
                    .unwrap() as u32;
                *resolved_offset = Some((new_offset, *target));
            }
        }
    }

    pub fn offset(&self) -> BranchOffset {
        BranchOffset::Offset(self.insns.len() as InsnReference)
    }

    pub fn allocate_label(&mut self) -> BranchOffset {
        let label_n = self.label_to_resolved_offset.len();
        self.label_to_resolved_offset.push(None);
        BranchOffset::Label(label_n as u32)
    }

    pub fn resolve_label(
        &mut self,
        label: BranchOffset,
        to_offset: BranchOffset,
        target: JumpTarget,
    ) {
        assert!(matches!(label, BranchOffset::Label(_)));
        assert!(matches!(to_offset, BranchOffset::Offset(_)));
        let BranchOffset::Label(label_number) = label else {
            unreachable!("Label is not a label");
        };
        self.label_to_resolved_offset[label_number as usize] =
            Some((to_offset.to_offset_int(), target));
    }

    /// Resolve unresolved labels to a specific offset in the instruction list.
    ///
    /// This function scans all instructions and resolves any labels to their corresponding offsets.
    /// It ensures that all labels are resolved correctly and updates the target program counter (PC)
    /// of each instruction that references a label.
    pub fn resolve_labels(&mut self) {
        let resolve = |pc: &mut BranchOffset, insn_name: &str| {
            if let BranchOffset::Label(label) = pc {
                let Some(Some((to_offset, target))) =
                    self.label_to_resolved_offset.get(*label as usize)
                else {
                    panic!(
                        "Reference to undefined or unresolved label in {}: {}",
                        insn_name, label
                    );
                };
                *pc = BranchOffset::Offset(
                    to_offset
                        + if *target == JumpTarget::NextInsn {
                            0
                        } else {
                            1
                        },
                );
            }
        };
        for (insn, _, _) in self.insns.iter_mut() {
            match insn {
                Insn::Init { target_pc } => {
                    resolve(target_pc, "Init");
                }
                Insn::Eq {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Eq");
                }
                Insn::Ne {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ne");
                }
                Insn::Lt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Lt");
                }
                Insn::Le {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Le");
                }
                Insn::Gt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Gt");
                }
                Insn::Ge {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ge");
                }
                Insn::If {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "If");
                }
                Insn::IfNot {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "IfNot");
                }
                Insn::Rewind { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Rewind");
                }
                Insn::Last { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Last");
                }
                Insn::Goto { target_pc } => {
                    resolve(target_pc, "Goto");
                }
                Insn::DecrJumpZero {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "DecrJumpZero");
                }
                Insn::SorterNext {
                    cursor_id: _cursor_id,
                    pc_if_next,
                } => {
                    resolve(pc_if_next, "SorterNext");
                }
                Insn::SorterSort { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "SorterSort");
                }
                Insn::NotNull {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "NotNull");
                }
                Insn::IfPos { target_pc, .. } => {
                    resolve(target_pc, "IfPos");
                }
                Insn::Next { pc_if_next, .. } => {
                    resolve(pc_if_next, "Next");
                }
                Insn::Prev { pc_if_prev, .. } => {
                    resolve(pc_if_prev, "Prev");
                }
                Insn::InitCoroutine {
                    yield_reg: _,
                    jump_on_definition,
                    start_offset,
                } => {
                    resolve(jump_on_definition, "InitCoroutine");
                    resolve(start_offset, "InitCoroutine");
                }
                Insn::NotExists {
                    cursor: _,
                    rowid_reg: _,
                    target_pc,
                } => {
                    resolve(target_pc, "NotExists");
                }
                Insn::Yield {
                    yield_reg: _,
                    end_offset,
                } => {
                    resolve(end_offset, "Yield");
                }
                Insn::SeekRowid { target_pc, .. } => {
                    resolve(target_pc, "SeekRowid");
                }
                Insn::Gosub { target_pc, .. } => {
                    resolve(target_pc, "Gosub");
                }
                Insn::Jump {
                    target_pc_eq,
                    target_pc_lt,
                    target_pc_gt,
                } => {
                    resolve(target_pc_eq, "Jump");
                    resolve(target_pc_lt, "Jump");
                    resolve(target_pc_gt, "Jump");
                }
                Insn::SeekGE { target_pc, .. } => {
                    resolve(target_pc, "SeekGE");
                }
                Insn::SeekGT { target_pc, .. } => {
                    resolve(target_pc, "SeekGT");
                }
                Insn::SeekLE { target_pc, .. } => {
                    resolve(target_pc, "SeekLE");
                }
                Insn::SeekLT { target_pc, .. } => {
                    resolve(target_pc, "SeekLT");
                }
                Insn::IdxGE { target_pc, .. } => {
                    resolve(target_pc, "IdxGE");
                }
                Insn::IdxLE { target_pc, .. } => {
                    resolve(target_pc, "IdxLE");
                }
                Insn::IdxGT { target_pc, .. } => {
                    resolve(target_pc, "IdxGT");
                }
                Insn::IdxLT { target_pc, .. } => {
                    resolve(target_pc, "IdxLT");
                }
                Insn::IsNull { reg: _, target_pc } => {
                    resolve(target_pc, "IsNull");
                }
                Insn::VNext { pc_if_next, .. } => {
                    resolve(pc_if_next, "VNext");
                }
                Insn::VFilter { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "VFilter");
                }
                _ => {}
            }
        }
        self.label_to_resolved_offset.clear();
    }

    // translate table to cursor id
    pub fn resolve_cursor_id_safe(&self, table_identifier: &str) -> Option<CursorID> {
        self.cursor_ref.iter().position(|(t_ident, _)| {
            t_ident
                .as_ref()
                .is_some_and(|ident| ident == table_identifier)
        })
    }

    pub fn resolve_cursor_id(&self, table_identifier: &str) -> CursorID {
        self.resolve_cursor_id_safe(table_identifier)
            .unwrap_or_else(|| panic!("Cursor not found: {}", table_identifier))
    }

    pub fn build(
        mut self,
        database_header: Arc<SpinLock<DatabaseHeader>>,
        connection: Weak<Connection>,
        change_cnt_on: bool,
    ) -> Program {
        self.resolve_labels();

        self.parameters.list.dedup();
        Program {
            max_registers: self.next_free_register,
            insns: self
                .insns
                .into_iter()
                .map(|(insn, function, _)| (insn, function))
                .collect(),
            cursor_ref: self.cursor_ref,
            database_header,
            comments: None,
            connection,
            parameters: self.parameters,
            n_change: Cell::new(0),
            change_cnt_on,
            result_columns: self.result_columns,
            table_references: self.table_references,
        }
    }
}
