#![allow(unused_variables)]
use crate::numeric::{NullableInteger, Numeric};
use crate::reusable_future_box::ReusableBoxFuture;
use crate::storage::btree;
use crate::storage::database::FileMemoryStorage;
use crate::storage::page_cache::DumbLruPageCache;
use crate::storage::pager::CreateBTreeFlags;
use crate::vdbe::create_waker;
use crate::{
    error::{LimboError, SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_PRIMARYKEY},
    ext::ExtValue,
    function::{AggFunc, ExtFunc, MathFunc, MathFuncArity, ScalarFunc, VectorFunc},
    functions::{
        datetime::{
            exec_date, exec_datetime_full, exec_julianday, exec_strftime, exec_time, exec_unixepoch,
        },
        printf::exec_printf,
    },
    types::compare_immutable,
};
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::{borrow::BorrowMut, rc::Rc, sync::Arc};

use crate::{pseudo::PseudoCursor, result::LimboResult};

use crate::{
    schema::{affinity, Affinity},
    storage::btree::{BTreeCursor, BTreeKey},
};

use crate::{
    storage::wal::CheckpointResult,
    types::{
        AggContext, Cursor, CursorResult, ExternalAggState, OwnedValue, OwnedValueType, SeekKey,
        SeekOp,
    },
    util::{
        cast_real_to_integer, cast_text_to_integer, cast_text_to_numeric, cast_text_to_real,
        checked_cast_text_to_numeric, parse_schema_rows, RoundToPrecision,
    },
    vdbe::{
        builder::CursorType,
        insn::{IdxInsertFlags, Insn},
    },
    vector::{vector32, vector64, vector_distance_cos, vector_extract},
};

use crate::{
    info, maybe_init_database_file, return_if_io, BufferPool, MvCursor, OpenFlags, RefValue, Row,
    StepResult, TransactionState, IO,
};

use super::{
    insn::{Cookie, RegisterOrLiteral},
    HaltState,
};
use parking_lot::RwLock;
use rand::thread_rng;

use super::{
    likeop::{construct_like_escape_arg, exec_glob, exec_like_with_escape},
    sorter::Sorter,
};
use regex::{Regex, RegexBuilder};
use std::{cell::RefCell, collections::HashMap};

#[cfg(feature = "json")]
use crate::{
    function::JsonFunc, json::convert_dbtype_to_raw_jsonb, json::get_json, json::is_json_valid,
    json::json_array, json::json_array_length, json::json_arrow_extract,
    json::json_arrow_shift_extract, json::json_error_position, json::json_extract,
    json::json_from_raw_bytes_agg, json::json_insert, json::json_object, json::json_patch,
    json::json_quote, json::json_remove, json::json_replace, json::json_set, json::json_type,
    json::jsonb, json::jsonb_array, json::jsonb_extract, json::jsonb_insert, json::jsonb_object,
    json::jsonb_patch, json::jsonb_remove, json::jsonb_replace, json::jsonb_set,
};

use super::{get_new_rowid, make_record, BranchOffset, Program, ProgramState, Register};
use crate::{
    bail_constraint_error, must_be_btree_cursor, resolve_ext_path, MvStore, Pager, Result,
    DATABASE_VERSION,
};

macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(InsnFunctionStepResult::IO),
        }
    };
}
pub type InsnFunction = fn(
    &Program,
    &mut ProgramState,
    &Insn,
    &Rc<Pager>,
    Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult>;

pub enum InsnFunctionStepResult {
    Done,
    IO,
    Row,
    Interrupt,
    Busy,
    Step,
}

pub fn op_init(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Init { target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    state.pc = target_pc.to_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Add { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_add(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_subtract(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Subtract { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_subtract(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_multiply(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Multiply { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_multiply(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_divide(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Divide { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_divide(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_remainder(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Remainder { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_remainder(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_and(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::BitAnd { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_bit_and(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_or(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::BitOr { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_bit_or(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::BitNot { reg, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] =
        Register::OwnedValue(exec_bit_not(state.registers[*reg].get_owned_value()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_checkpoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Checkpoint {
        database: _,
        checkpoint_mode: _,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let result = program.connection.upgrade().unwrap().checkpoint();
    match result {
        Ok(CheckpointResult {
            num_wal_frames: num_wal_pages,
            num_checkpointed_frames: num_checkpointed_pages,
        }) => {
            // https://sqlite.org/pragma.html#pragma_wal_checkpoint
            // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
            state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(0));
            // 2nd col: # modified pages written to wal file
            state.registers[*dest + 1] =
                Register::OwnedValue(OwnedValue::Integer(num_wal_pages as i64));
            // 3rd col: # pages moved to db after checkpoint
            state.registers[*dest + 2] =
                Register::OwnedValue(OwnedValue::Integer(num_checkpointed_pages as i64));
        }
        Err(_err) => state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(1)),
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Null { dest, dest_end } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Some(dest_end) = dest_end {
        for i in *dest..=*dest_end {
            state.registers[i] = Register::OwnedValue(OwnedValue::Null);
        }
    } else {
        state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_null_row(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NullRow { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "NullRow");
        let cursor = cursor.as_btree_mut();
        cursor.set_null_flag(true);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_compare(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Compare {
        start_reg_a,
        start_reg_b,
        count,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let start_reg_a = *start_reg_a;
    let start_reg_b = *start_reg_b;
    let count = *count;

    if start_reg_a + count > start_reg_b {
        return Err(LimboError::InternalError(
            "Compare registers overlap".to_string(),
        ));
    }

    let mut cmp = None;
    for i in 0..count {
        let a = state.registers[start_reg_a + i].get_owned_value();
        let b = state.registers[start_reg_b + i].get_owned_value();
        cmp = Some(a.cmp(b));
        if cmp != Some(std::cmp::Ordering::Equal) {
            break;
        }
    }
    state.last_compare = cmp;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_jump(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Jump {
        target_pc_lt,
        target_pc_eq,
        target_pc_gt,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc_lt.is_offset());
    assert!(target_pc_eq.is_offset());
    assert!(target_pc_gt.is_offset());
    let cmp = state.last_compare.take();
    if cmp.is_none() {
        return Err(LimboError::InternalError(
            "Jump without compare".to_string(),
        ));
    }
    let target_pc = match cmp.unwrap() {
        std::cmp::Ordering::Less => *target_pc_lt,
        std::cmp::Ordering::Equal => *target_pc_eq,
        std::cmp::Ordering::Greater => *target_pc_gt,
    };
    state.pc = target_pc.to_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_move(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Move {
        source_reg,
        dest_reg,
        count,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let source_reg = *source_reg;
    let dest_reg = *dest_reg;
    let count = *count;
    for i in 0..count {
        state.registers[dest_reg + i] = std::mem::replace(
            &mut state.registers[source_reg + i],
            Register::OwnedValue(OwnedValue::Null),
        );
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_pos(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IfPos {
        reg,
        target_pc,
        decrement_by,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let reg = *reg;
    let target_pc = *target_pc;
    match state.registers[reg].get_owned_value() {
        OwnedValue::Integer(n) if *n > 0 => {
            state.pc = target_pc.to_offset_int();
            state.registers[reg] =
                Register::OwnedValue(OwnedValue::Integer(*n - *decrement_by as i64));
        }
        OwnedValue::Integer(_) => {
            state.pc += 1;
        }
        _ => {
            return Err(LimboError::InternalError(
                "IfPos: the value in the register is not an integer".into(),
            ));
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NotNull { reg, target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let reg = *reg;
    let target_pc = *target_pc;
    match &state.registers[reg].get_owned_value() {
        OwnedValue::Null => {
            state.pc += 1;
        }
        _ => {
            state.pc = target_pc.to_offset_int();
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_eq(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Eq {
        lhs,
        rhs,
        target_pc,
        flags,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let lhs = *lhs;
    let rhs = *rhs;
    let target_pc = *target_pc;
    let cond = *state.registers[lhs].get_owned_value() == *state.registers[rhs].get_owned_value();
    let nulleq = flags.has_nulleq();
    let jump_if_null = flags.has_jump_if_null();
    match (
        &state.registers[lhs].get_owned_value(),
        &state.registers[rhs].get_owned_value(),
    ) {
        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
            if (nulleq && cond) || (!nulleq && jump_if_null) {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => {
            if *state.registers[lhs].get_owned_value() == *state.registers[rhs].get_owned_value() {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_ne(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Ne {
        lhs,
        rhs,
        target_pc,
        flags,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let lhs = *lhs;
    let rhs = *rhs;
    let target_pc = *target_pc;
    let cond = *state.registers[lhs].get_owned_value() != *state.registers[rhs].get_owned_value();
    let nulleq = flags.has_nulleq();
    let jump_if_null = flags.has_jump_if_null();
    match (
        &state.registers[lhs].get_owned_value(),
        &state.registers[rhs].get_owned_value(),
    ) {
        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
            if (nulleq && cond) || (!nulleq && jump_if_null) {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => {
            if *state.registers[lhs].get_owned_value() != *state.registers[rhs].get_owned_value() {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_lt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Lt {
        lhs,
        rhs,
        target_pc,
        flags,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let lhs = *lhs;
    let rhs = *rhs;
    let target_pc = *target_pc;
    let jump_if_null = flags.has_jump_if_null();
    match (
        &state.registers[lhs].get_owned_value(),
        &state.registers[rhs].get_owned_value(),
    ) {
        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
            if jump_if_null {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => {
            if *state.registers[lhs].get_owned_value() < *state.registers[rhs].get_owned_value() {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_le(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Le {
        lhs,
        rhs,
        target_pc,
        flags,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let lhs = *lhs;
    let rhs = *rhs;
    let target_pc = *target_pc;
    let jump_if_null = flags.has_jump_if_null();
    match (
        &state.registers[lhs].get_owned_value(),
        &state.registers[rhs].get_owned_value(),
    ) {
        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
            if jump_if_null {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => {
            if *state.registers[lhs].get_owned_value() <= *state.registers[rhs].get_owned_value() {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_gt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Gt {
        lhs,
        rhs,
        target_pc,
        flags,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let lhs = *lhs;
    let rhs = *rhs;
    let target_pc = *target_pc;
    let jump_if_null = flags.has_jump_if_null();
    match (
        &state.registers[lhs].get_owned_value(),
        &state.registers[rhs].get_owned_value(),
    ) {
        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
            if jump_if_null {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => {
            if *state.registers[lhs].get_owned_value() > *state.registers[rhs].get_owned_value() {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_ge(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Ge {
        lhs,
        rhs,
        target_pc,
        flags,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let lhs = *lhs;
    let rhs = *rhs;
    let target_pc = *target_pc;
    let jump_if_null = flags.has_jump_if_null();
    match (
        &state.registers[lhs].get_owned_value(),
        &state.registers[rhs].get_owned_value(),
    ) {
        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
            if jump_if_null {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
        _ => {
            if *state.registers[lhs].get_owned_value() >= *state.registers[rhs].get_owned_value() {
                state.pc = target_pc.to_offset_int();
            } else {
                state.pc += 1;
            }
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::If {
        reg,
        target_pc,
        jump_if_null,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    if exec_if(
        &state.registers[*reg].get_owned_value(),
        *jump_if_null,
        false,
    ) {
        state.pc = target_pc.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IfNot {
        reg,
        target_pc,
        jump_if_null,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    if exec_if(
        &state.registers[*reg].get_owned_value(),
        *jump_if_null,
        true,
    ) {
        state.pc = target_pc.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_read(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OpenRead {
        cursor_id,
        root_page,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let mv_cursor = match state.mv_tx_id {
        Some(tx_id) => {
            let table_id = *root_page as u64;
            let mv_store = mv_store.unwrap().clone();
            let mv_cursor = Rc::new(RefCell::new(
                MvCursor::new(mv_store.clone(), tx_id, table_id).unwrap(),
            ));
            Some(mv_cursor)
        }
        None => None,
    };
    let mut cursors = state.cursors.borrow_mut();
    match cursor_type {
        CursorType::BTreeTable(_) => {
            let cursor = BTreeCursor::new(mv_cursor, pager.clone(), *root_page);
            cursors
                .get_mut(*cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(index) => {
            let cursor =
                BTreeCursor::new_index(mv_cursor, pager.clone(), *root_page, index.as_ref());
            cursors
                .get_mut(*cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::Pseudo(_) => {
            panic!("OpenRead on pseudo cursor");
        }
        CursorType::Sorter => {
            panic!("OpenRead on sorter cursor");
        }
        CursorType::VirtualTable(_) => {
            panic!("OpenRead on virtual table cursor, use Insn:VOpen instead");
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vopen(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VOpen { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VOpen on non-virtual table cursor");
    };
    let cursor = virtual_table.open()?;
    state
        .cursors
        .borrow_mut()
        .insert(*cursor_id, Some(Cursor::Virtual(cursor)));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcreate(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VCreate {
        module_name,
        table_name,
        args_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let module_name = state.registers[*module_name].get_owned_value().to_string();
    let table_name = state.registers[*table_name].get_owned_value().to_string();
    let args = if let Some(args_reg) = args_reg {
        if let Register::Record(rec) = &state.registers[*args_reg] {
            rec.get_values().iter().map(|v| v.to_ffi()).collect()
        } else {
            return Err(LimboError::InternalError(
                "VCreate: args_reg is not a record".to_string(),
            ));
        }
    } else {
        vec![]
    };
    let Some(conn) = program.connection.upgrade() else {
        return Err(crate::LimboError::ExtensionError(
            "Failed to upgrade Connection".to_string(),
        ));
    };
    let mod_type = conn
        .syms
        .borrow()
        .vtab_modules
        .get(&module_name)
        .ok_or_else(|| {
            crate::LimboError::ExtensionError(format!("Module {} not found", module_name))
        })?
        .module_kind;
    let table = crate::VirtualTable::from_args(
        Some(&table_name),
        &module_name,
        args,
        &conn.syms.borrow(),
        mod_type,
        None,
    )?;
    {
        conn.syms
            .borrow_mut()
            .vtabs
            .insert(table_name, table.clone());
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vfilter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VFilter {
        cursor_id,
        pc_if_empty,
        arg_count,
        args_reg,
        idx_str,
        idx_num,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VFilter on non-virtual table cursor");
    };
    let has_rows = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        let mut args = Vec::with_capacity(*arg_count);
        for i in 0..*arg_count {
            args.push(
                state.registers[args_reg + i]
                    .get_owned_value()
                    .clone()
                    .to_ffi(),
            );
        }
        let idx_str = if let Some(idx_str) = idx_str {
            Some(state.registers[*idx_str].get_owned_value().to_string())
        } else {
            None
        };
        virtual_table.filter(cursor, *idx_num as i32, idx_str, *arg_count, args)?
    };
    if !has_rows {
        state.pc = pc_if_empty.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcolumn(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VColumn {
        cursor_id,
        column,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VColumn on non-virtual table cursor");
    };
    let value = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        virtual_table.column(cursor, *column)?
    };
    state.registers[*dest] = Register::OwnedValue(value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vupdate(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VUpdate {
        cursor_id,
        arg_count,
        start_reg,
        conflict_action,
        ..
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VUpdate on non-virtual table cursor");
    };

    if *arg_count < 2 {
        return Err(LimboError::InternalError(
            "VUpdate: arg_count must be at least 2 (rowid and insert_rowid)".to_string(),
        ));
    }
    let mut argv = Vec::with_capacity(*arg_count);
    for i in 0..*arg_count {
        if let Some(value) = state.registers.get(*start_reg + i) {
            argv.push(value.get_owned_value().clone());
        } else {
            return Err(LimboError::InternalError(format!(
                "VUpdate: register out of bounds at {}",
                *start_reg + i
            )));
        }
    }
    let result = virtual_table.update(&argv);
    match result {
        Ok(Some(new_rowid)) => {
            if *conflict_action == 5 {
                // ResolveType::Replace
                if let Some(conn) = program.connection.upgrade() {
                    conn.update_last_rowid(new_rowid as u64);
                }
            }
            state.pc += 1;
        }
        Ok(None) => {
            // no-op or successful update without rowid return
            state.pc += 1;
        }
        Err(e) => {
            // virtual table update failed
            return Err(LimboError::ExtensionError(format!(
                "Virtual table update failed: {}",
                e
            )));
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vnext(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VNext {
        cursor_id,
        pc_if_next,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VNext on non-virtual table cursor");
    };
    let has_more = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        virtual_table.next(cursor)?
    };
    if has_more {
        state.pc = pc_if_next.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vdestroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::VDestroy { db, table_name } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let Some(conn) = program.connection.upgrade() else {
        return Err(crate::LimboError::ExtensionError(
            "Failed to upgrade Connection".to_string(),
        ));
    };

    {
        let Some(vtab) = conn.syms.borrow_mut().vtabs.remove(table_name) else {
            return Err(crate::LimboError::InternalError(
                "Could not find Virtual Table to Destroy".to_string(),
            ));
        };
        vtab.destroy()?;
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_pseudo(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OpenPseudo {
        cursor_id,
        content_reg: _,
        num_fields: _,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursors = state.cursors.borrow_mut();
        let cursor = PseudoCursor::new();
        cursors
            .get_mut(*cursor_id)
            .unwrap()
            .replace(Cursor::new_pseudo(cursor));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_rewind(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Rewind {
        cursor_id,
        pc_if_empty,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Rewind");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.rewind());
        cursor.is_empty()
    };
    if is_empty {
        state.pc = pc_if_empty.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_last(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Last {
        cursor_id,
        pc_if_empty,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Last");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.last());
        cursor.is_empty()
    };
    if is_empty {
        state.pc = pc_if_empty.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Column {
        cursor_id,
        column,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
        let deferred_seek = {
            let rowid = {
                let mut index_cursor = state.get_cursor(index_cursor_id);
                let index_cursor = index_cursor.as_btree_mut();
                index_cursor.rowid()?
            };
            let mut table_cursor = state.get_cursor(table_cursor_id);
            let table_cursor = table_cursor.as_btree_mut();
            match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                CursorResult::Ok(_) => None,
                CursorResult::IO => Some((index_cursor_id, table_cursor_id)),
            }
        };
        if let Some(deferred_seek) = deferred_seek {
            state.deferred_seek = Some(deferred_seek);
            return Ok(InsnFunctionStepResult::IO);
        }
    }
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    match cursor_type {
        CursorType::BTreeTable(_) | CursorType::BTreeIndex(_) => {
            let value = {
                let mut cursor =
                    must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Column");
                let cursor = cursor.as_btree_mut();
                let record = cursor.record();
                let value = if let Some(record) = record.as_ref() {
                    if cursor.get_null_flag() {
                        RefValue::Null
                    } else {
                        match record.get_value_opt(*column) {
                            Some(val) => val.clone(),
                            None => RefValue::Null,
                        }
                    }
                } else {
                    RefValue::Null
                };
                value
            };
            // If we are copying a text/blob, let's try to simply update size of text if we need to allocate more and reuse.
            match (&value, &mut state.registers[*dest]) {
                (RefValue::Text(text_ref), Register::OwnedValue(OwnedValue::Text(text_reg))) => {
                    text_reg.value.clear();
                    text_reg.value.extend_from_slice(text_ref.value.to_slice());
                }
                (RefValue::Blob(raw_slice), Register::OwnedValue(OwnedValue::Blob(blob_reg))) => {
                    blob_reg.clear();
                    blob_reg.extend_from_slice(raw_slice.to_slice());
                }
                _ => {
                    let reg = &mut state.registers[*dest];
                    *reg = Register::OwnedValue(value.to_owned());
                }
            }
        }
        CursorType::Sorter => {
            let record = {
                let mut cursor = state.get_cursor(*cursor_id);
                let cursor = cursor.as_sorter_mut();
                cursor.record().cloned()
            };
            if let Some(record) = record {
                state.registers[*dest] =
                    Register::OwnedValue(match record.get_value_opt(*column) {
                        Some(val) => val.to_owned(),
                        None => OwnedValue::Null,
                    });
            } else {
                state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
            }
        }
        CursorType::Pseudo(_) => {
            let value = {
                let mut cursor = state.get_cursor(*cursor_id);
                let cursor = cursor.as_pseudo_mut();
                if let Some(record) = cursor.record() {
                    record.get_value(*column).to_owned()
                } else {
                    OwnedValue::Null
                }
            };
            state.registers[*dest] = Register::OwnedValue(value);
        }
        CursorType::VirtualTable(_) => {
            panic!("Insn:Column on virtual table cursor, use Insn:VColumn instead");
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_type_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::TypeCheck {
        start_reg,
        count,
        check_generated,
        table_reference,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert_eq!(table_reference.is_strict, true);
    state.registers[*start_reg..*start_reg + *count]
        .iter_mut()
        .zip(table_reference.columns.iter())
        .try_for_each(|(reg, col)| {
            // INT PRIMARY KEY is not row_id_alias so we throw error if this col is NULL
            if !col.is_rowid_alias
                && col.primary_key
                && matches!(reg.get_owned_value(), OwnedValue::Null)
            {
                bail_constraint_error!(
                    "NOT NULL constraint failed: {}.{} ({})",
                    &table_reference.name,
                    col.name.as_ref().map(|s| s.as_str()).unwrap_or(""),
                    SQLITE_CONSTRAINT
                )
            } else if col.is_rowid_alias && matches!(reg.get_owned_value(), OwnedValue::Null) {
                // Handle INTEGER PRIMARY KEY for null as usual (Rowid will be auto-assigned)
                return Ok(());
            }
            let col_affinity = col.affinity();
            let ty_str = col.ty_str.as_str();
            let applied = apply_affinity_char(reg, col_affinity);
            let value_type = reg.get_owned_value().value_type();
            match (ty_str, value_type) {
                ("INTEGER" | "INT", OwnedValueType::Integer) => {}
                ("REAL", OwnedValueType::Float) => {}
                ("BLOB", OwnedValueType::Blob) => {}
                ("TEXT", OwnedValueType::Text) => {}
                ("ANY", _) => {}
                (t, v) => bail_constraint_error!(
                    "cannot store {} value in {} column {}.{} ({})",
                    v,
                    t,
                    &table_reference.name,
                    col.name.as_ref().map(|s| s.as_str()).unwrap_or(""),
                    SQLITE_CONSTRAINT
                ),
            };
            Ok(())
        })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_make_record(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::MakeRecord {
        start_reg,
        count,
        dest_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let record = make_record(&state.registers, start_reg, count);
    state.registers[*dest_reg] = Register::Record(record);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_result_row(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ResultRow { start_reg, count } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let row = Row {
        values: &state.registers[*start_reg] as *const Register,
        count: *count,
    };

    state.result_row = Some(row);
    state.pc += 1;
    return Ok(InsnFunctionStepResult::Row);
}

pub fn op_next(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let state_ptr = state as *mut ProgramState;
    if let Some((_, _, future)) = &mut (unsafe { &mut *state_ptr }).func {
        let waker = create_waker();
        let mut cx = Context::from_waker(&waker);
        match future.as_mut().poll(&mut cx) {
            std::task::Poll::Pending => {
                return Ok(InsnFunctionStepResult::Step);
            }
            std::task::Poll::Ready(_) => {}
        }
        (unsafe { &mut *state_ptr }).func.take();
        return Ok(InsnFunctionStepResult::Step);
    } else {
        let Insn::Next {
            cursor_id,
            pc_if_next,
        } = insn
        else {
            unreachable!("unexpected Insn {:?}", insn)
        };
        assert!(pc_if_next.is_offset());
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Next");
        let cursor = cursor.as_btree_mut();
        let cursor_static = cursor as *mut BTreeCursor;
        cursor.set_null_flag(false);
        let mut future = Box::pin(op_next_async(state_ptr, cursor, *pc_if_next));
        let waker = create_waker();
        let mut cx = Context::from_waker(&waker);
        match future.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(value) => match value? {
                CursorResult::Ok(()) => return Ok(InsnFunctionStepResult::Step),
                CursorResult::IO => {}
            },
            std::task::Poll::Pending => {}
        }
        (unsafe { &mut *state_ptr }).func = Some((false, cursor_static, future));
        Ok(InsnFunctionStepResult::IO)
    }
}

#[inline(always)]
async fn op_next_async(
    state: *mut ProgramState,
    cursor: *mut BTreeCursor,
    pc_if_next: BranchOffset,
) -> Result<CursorResult<()>> {
    unsafe {
        let rowid = match (&mut *cursor).get_next_record_async(None).await.unwrap() {
            CursorResult::Ok(v) => v,
            CursorResult::IO => panic!("WTF"),
        };
        (&mut *cursor).rowid.replace(rowid);
        (&mut *cursor).empty_record.replace(rowid.is_none());
        let is_empty = { (&mut *cursor).is_empty() };
        if !is_empty {
            (*state).pc = pc_if_next.to_offset_int();
        } else {
            (*state).pc += 1;
        }
    }
    Ok(CursorResult::Ok(()))
}

pub fn op_prev(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Prev {
        cursor_id,
        pc_if_prev,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_prev.is_offset());
    let is_empty = {
        let mut cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Prev");
        let cursor = cursor.as_btree_mut();
        cursor.set_null_flag(false);
        return_if_io!(cursor.prev());

        cursor.is_empty()
    };
    if !is_empty {
        state.pc = pc_if_prev.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_halt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Halt {
        err_code,
        description,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    match *err_code {
        0 => {}
        SQLITE_CONSTRAINT_PRIMARYKEY => {
            return Err(LimboError::Constraint(format!(
                "UNIQUE constraint failed: {} (19)",
                description
            )));
        }
        _ => {
            return Err(LimboError::Constraint(format!(
                "undocumented halt error code {}",
                description
            )));
        }
    }
    match program.halt(pager.clone(), state, mv_store)? {
        StepResult::Done => Ok(InsnFunctionStepResult::Done),
        StepResult::IO => Ok(InsnFunctionStepResult::IO),
        StepResult::Row => Ok(InsnFunctionStepResult::Row),
        StepResult::Interrupt => Ok(InsnFunctionStepResult::Interrupt),
        StepResult::Busy => Ok(InsnFunctionStepResult::Busy),
    }
}

pub fn op_transaction(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Transaction { write } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Some(mv_store) = &mv_store {
        if state.mv_tx_id.is_none() {
            let tx_id = mv_store.begin_tx();
            program
                .connection
                .upgrade()
                .unwrap()
                .mv_transactions
                .borrow_mut()
                .push(tx_id);
            state.mv_tx_id = Some(tx_id);
        }
    } else {
        let connection = program.connection.upgrade().unwrap();
        let current_state = connection.transaction_state.get();
        let (new_transaction_state, updated) = match (current_state, write) {
            (TransactionState::Write, true) => (TransactionState::Write, false),
            (TransactionState::Write, false) => (TransactionState::Write, false),
            (TransactionState::Read, true) => (TransactionState::Write, true),
            (TransactionState::Read, false) => (TransactionState::Read, false),
            (TransactionState::None, true) => (TransactionState::Write, true),
            (TransactionState::None, false) => (TransactionState::Read, true),
        };

        if updated && matches!(current_state, TransactionState::None) {
            if let LimboResult::Busy = pager.begin_read_tx()? {
                return Ok(InsnFunctionStepResult::Busy);
            }
        }

        if updated && matches!(new_transaction_state, TransactionState::Write) {
            if let LimboResult::Busy = pager.begin_write_tx()? {
                tracing::trace!("begin_write_tx busy");
                return Ok(InsnFunctionStepResult::Busy);
            }
        }
        if updated {
            connection.transaction_state.replace(new_transaction_state);
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_auto_commit(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AutoCommit {
        auto_commit,
        rollback,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let conn = program.connection.upgrade().unwrap();
    if matches!(state.halt_state, Some(HaltState::Checkpointing)) {
        return match program.halt(pager.clone(), state, mv_store)? {
            super::StepResult::Done => Ok(InsnFunctionStepResult::Done),
            super::StepResult::IO => Ok(InsnFunctionStepResult::IO),
            super::StepResult::Row => Ok(InsnFunctionStepResult::Row),
            super::StepResult::Interrupt => Ok(InsnFunctionStepResult::Interrupt),
            super::StepResult::Busy => Ok(InsnFunctionStepResult::Busy),
        };
    }

    if *auto_commit != conn.auto_commit.get() {
        if *rollback {
            todo!("Rollback is not implemented");
        } else {
            conn.auto_commit.replace(*auto_commit);
        }
    } else if !*auto_commit {
        return Err(LimboError::TxError(
            "cannot start a transaction within a transaction".to_string(),
        ));
    } else if *rollback {
        return Err(LimboError::TxError(
            "cannot rollback - no transaction is active".to_string(),
        ));
    } else {
        return Err(LimboError::TxError(
            "cannot commit - no transaction is active".to_string(),
        ));
    }
    return match program.halt(pager.clone(), state, mv_store)? {
        super::StepResult::Done => Ok(InsnFunctionStepResult::Done),
        super::StepResult::IO => Ok(InsnFunctionStepResult::IO),
        super::StepResult::Row => Ok(InsnFunctionStepResult::Row),
        super::StepResult::Interrupt => Ok(InsnFunctionStepResult::Interrupt),
        super::StepResult::Busy => Ok(InsnFunctionStepResult::Busy),
    };
}

pub fn op_goto(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Goto { target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    state.pc = target_pc.to_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_gosub(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Gosub {
        target_pc,
        return_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    state.registers[*return_reg] = Register::OwnedValue(OwnedValue::Integer((state.pc + 1) as i64));
    state.pc = target_pc.to_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_return(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Return { return_reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let OwnedValue::Integer(pc) = state.registers[*return_reg].get_owned_value() {
        let pc: u32 = (*pc)
            .try_into()
            .unwrap_or_else(|_| panic!("Return register is negative: {}", pc));
        state.pc = pc;
    } else {
        return Err(LimboError::InternalError(
            "Return register is not an integer".to_string(),
        ));
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_integer(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Integer { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Real { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(OwnedValue::Float(*value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real_affinity(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::RealAffinity { register } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let OwnedValue::Integer(i) = &state.registers[*register].get_owned_value() {
        state.registers[*register] = Register::OwnedValue(OwnedValue::Float(*i as f64));
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_string8(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::String8 { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(OwnedValue::build_text(value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_blob(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Blob { value, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(OwnedValue::Blob(value.clone()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_row_id(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::RowId { cursor_id, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
        let deferred_seek = {
            let rowid = {
                let mut index_cursor = state.get_cursor(index_cursor_id);
                let index_cursor = index_cursor.as_btree_mut();
                index_cursor.rowid()?
            };
            let mut table_cursor = state.get_cursor(table_cursor_id);
            let table_cursor = table_cursor.as_btree_mut();
            match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                CursorResult::Ok(_) => None,
                CursorResult::IO => Some((index_cursor_id, table_cursor_id)),
            }
        };
        if let Some(deferred_seek) = deferred_seek {
            state.deferred_seek = Some(deferred_seek);
            return Ok(InsnFunctionStepResult::IO);
        }
    }
    let mut cursors = state.cursors.borrow_mut();
    if let Some(Cursor::BTree(btree_cursor)) = cursors.get_mut(*cursor_id).unwrap() {
        if let Some(ref rowid) = btree_cursor.rowid()? {
            state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(*rowid as i64));
        } else {
            state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
        }
    } else if let Some(Cursor::Virtual(virtual_cursor)) = cursors.get_mut(*cursor_id).unwrap() {
        let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
        let CursorType::VirtualTable(virtual_table) = cursor_type else {
            panic!("VUpdate on non-virtual table cursor");
        };
        let rowid = virtual_table.rowid(virtual_cursor);
        if rowid != 0 {
            state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(rowid));
        } else {
            state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
        }
    } else {
        return Err(LimboError::InternalError(
            "RowId: cursor is not a table or virtual cursor".to_string(),
        ));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_row_id(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxRowId { cursor_id, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let mut cursors = state.cursors.borrow_mut();
    let cursor = cursors.get_mut(*cursor_id).unwrap().as_mut().unwrap();
    let cursor = cursor.as_btree_mut();
    let rowid = cursor.rowid()?;
    state.registers[*dest] = match rowid {
        Some(rowid) => Register::OwnedValue(OwnedValue::Integer(rowid as i64)),
        None => Register::OwnedValue(OwnedValue::Null),
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_rowid(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SeekRowid {
        cursor_id,
        src_reg,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        let rowid = match state.registers[*src_reg].get_owned_value() {
            OwnedValue::Integer(rowid) => Some(*rowid as u64),
            OwnedValue::Null => None,
            other => {
                return Err(LimboError::InternalError(format!(
                    "SeekRowid: the value in the register is not an integer or NULL: {}",
                    other
                )));
            }
        };
        match rowid {
            Some(rowid) => {
                let found = return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ));
                if !found {
                    target_pc.to_offset_int()
                } else {
                    state.pc + 1
                }
            }
            None => target_pc.to_offset_int(),
        }
    };
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_deferred_seek(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DeferredSeek {
        index_cursor_id,
        table_cursor_id,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.deferred_seek = Some((*index_cursor_id, *table_cursor_id));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (Insn::SeekGE {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
        is_index,
    }
    | Insn::SeekGT {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
        is_index,
    }
    | Insn::SeekLE {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
        is_index,
    }
    | Insn::SeekLT {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
        is_index,
    }) = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(
        target_pc.is_offset(),
        "target_pc should be an offset, is: {:?}",
        target_pc
    );
    let op = match insn {
        Insn::SeekGE { .. } => SeekOp::GE,
        Insn::SeekGT { .. } => SeekOp::GT,
        Insn::SeekLE { .. } => SeekOp::LE,
        Insn::SeekLT { .. } => SeekOp::LT,
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    let op_name = match op {
        SeekOp::GE => "SeekGE",
        SeekOp::GT => "SeekGT",
        SeekOp::LE => "SeekLE",
        SeekOp::LT => "SeekLT",
        _ => unreachable!("unexpected SeekOp {:?}", op),
    };
    if *is_index {
        let found = {
            let mut cursor = state.get_cursor(*cursor_id);
            let cursor = cursor.as_btree_mut();
            let record_from_regs = make_record(&state.registers, start_reg, num_regs);
            let found = return_if_io!(cursor.seek(SeekKey::IndexKey(&record_from_regs), op));
            found
        };
        if !found {
            state.pc = target_pc.to_offset_int();
        } else {
            state.pc += 1;
        }
    } else {
        let pc = {
            let mut cursor = state.get_cursor(*cursor_id);
            let cursor = cursor.as_btree_mut();
            let rowid = match state.registers[*start_reg].get_owned_value() {
                OwnedValue::Null => {
                    // All integer values are greater than null so we just rewind the cursor
                    return_if_io!(cursor.rewind());
                    None
                }
                OwnedValue::Integer(rowid) => Some(*rowid as u64),
                _ => {
                    return Err(LimboError::InternalError(format!(
                        "{}: the value in the register is not an integer",
                        op_name
                    )));
                }
            };
            let found = match rowid {
                Some(rowid) => {
                    let found = return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), op));
                    if !found {
                        target_pc.to_offset_int()
                    } else {
                        state.pc + 1
                    }
                }
                None => state.pc + 1,
            };
            found
        };
        state.pc = pc;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_ge(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxGE {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        let record_from_regs = make_record(&state.registers, start_reg, num_regs);
        let pc = if let Some(ref idx_record) = *cursor.record() {
            // Compare against the same number of values
            let idx_values = idx_record.get_values();
            let idx_values = &idx_values[..record_from_regs.len()];
            let record_values = record_from_regs.get_values();
            let record_values = &record_values[..idx_values.len()];
            let ord = compare_immutable(&idx_values, &record_values, cursor.index_key_sort_order);
            if ord.is_ge() {
                target_pc.to_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            target_pc.to_offset_int()
        };
        pc
    };
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_end(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    if let Insn::SeekEnd { cursor_id } = *insn {
        let mut cursor = state.get_cursor(cursor_id);
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.seek_end());
    } else {
        unreachable!("unexpected Insn {:?}", insn)
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_le(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxLE {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        let record_from_regs = make_record(&state.registers, start_reg, num_regs);
        let pc = if let Some(ref idx_record) = *cursor.record() {
            // Compare against the same number of values
            let idx_values = idx_record.get_values();
            let idx_values = &idx_values[..record_from_regs.len()];
            let record_values = record_from_regs.get_values();
            let ord = compare_immutable(&idx_values, &record_values, cursor.index_key_sort_order);
            if ord.is_le() {
                target_pc.to_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            target_pc.to_offset_int()
        };
        pc
    };
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_gt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxGT {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        let record_from_regs = make_record(&state.registers, start_reg, num_regs);
        let pc = if let Some(ref idx_record) = *cursor.record() {
            // Compare against the same number of values
            let idx_values = idx_record.get_values();
            let idx_values = &idx_values[..record_from_regs.len()];
            let record_values = record_from_regs.get_values();
            let ord = compare_immutable(&idx_values, &record_values, cursor.index_key_sort_order);
            if ord.is_gt() {
                target_pc.to_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            target_pc.to_offset_int()
        };
        pc
    };
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_lt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IdxLT {
        cursor_id,
        start_reg,
        num_regs,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    let pc = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        let record_from_regs = make_record(&state.registers, start_reg, num_regs);
        let pc = if let Some(ref idx_record) = *cursor.record() {
            // Compare against the same number of values
            let idx_values = idx_record.get_values();
            let idx_values = &idx_values[..record_from_regs.len()];
            let record_values = record_from_regs.get_values();
            let ord = compare_immutable(&idx_values, &record_values, cursor.index_key_sort_order);
            if ord.is_lt() {
                target_pc.to_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            target_pc.to_offset_int()
        };
        pc
    };
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_decr_jump_zero(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DecrJumpZero { reg, target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(target_pc.is_offset());
    match state.registers[*reg].get_owned_value() {
        OwnedValue::Integer(n) => {
            let n = n - 1;
            if n == 0 {
                state.pc = target_pc.to_offset_int();
            } else {
                state.registers[*reg] = Register::OwnedValue(OwnedValue::Integer(n));
                state.pc += 1;
            }
        }
        _ => unreachable!("DecrJumpZero on non-integer register"),
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_agg_step(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AggStep {
        acc_reg,
        col,
        delimiter,
        func,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let Register::OwnedValue(OwnedValue::Null) = state.registers[*acc_reg] {
        state.registers[*acc_reg] = match func {
            AggFunc::Avg => Register::Aggregate(AggContext::Avg(
                OwnedValue::Float(0.0),
                OwnedValue::Integer(0),
            )),
            AggFunc::Sum => Register::Aggregate(AggContext::Sum(OwnedValue::Null)),
            AggFunc::Total => {
                // The result of total() is always a floating point value.
                // No overflow error is ever raised if any prior input was a floating point value.
                // Total() never throws an integer overflow.
                Register::Aggregate(AggContext::Sum(OwnedValue::Float(0.0)))
            }
            AggFunc::Count | AggFunc::Count0 => {
                Register::Aggregate(AggContext::Count(OwnedValue::Integer(0)))
            }
            AggFunc::Max => {
                let col = state.registers[*col].get_owned_value();
                match col {
                    OwnedValue::Integer(_) => Register::Aggregate(AggContext::Max(None)),
                    OwnedValue::Float(_) => Register::Aggregate(AggContext::Max(None)),
                    OwnedValue::Text(_) => Register::Aggregate(AggContext::Max(None)),
                    _ => {
                        unreachable!();
                    }
                }
            }
            AggFunc::Min => {
                let col = state.registers[*col].get_owned_value();
                match col {
                    OwnedValue::Integer(_) => Register::Aggregate(AggContext::Min(None)),
                    OwnedValue::Float(_) => Register::Aggregate(AggContext::Min(None)),
                    OwnedValue::Text(_) => Register::Aggregate(AggContext::Min(None)),
                    _ => {
                        unreachable!();
                    }
                }
            }
            AggFunc::GroupConcat | AggFunc::StringAgg => {
                Register::Aggregate(AggContext::GroupConcat(OwnedValue::build_text("")))
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
                Register::Aggregate(AggContext::GroupConcat(OwnedValue::Blob(vec![])))
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                Register::Aggregate(AggContext::GroupConcat(OwnedValue::Blob(vec![])))
            }
            AggFunc::External(func) => match func.as_ref() {
                ExtFunc::Aggregate {
                    init,
                    step,
                    finalize,
                    argc,
                } => Register::Aggregate(AggContext::External(ExternalAggState {
                    state: unsafe { (init)() },
                    argc: *argc,
                    step_fn: *step,
                    finalize_fn: *finalize,
                    finalized_value: None,
                })),
                _ => unreachable!("scalar function called in aggregate context"),
            },
        };
    }
    match func {
        AggFunc::Avg => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                unreachable!();
            };
            *acc = exec_add(acc, col.get_owned_value());
            *count += 1;
        }
        AggFunc::Sum | AggFunc::Total => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::Sum(acc) = agg.borrow_mut() else {
                unreachable!();
            };
            match col {
                Register::OwnedValue(owned_value) => {
                    *acc += owned_value;
                }
                _ => unreachable!(),
            }
        }
        AggFunc::Count | AggFunc::Count0 => {
            let col = state.registers[*col].get_owned_value().clone();
            if matches!(
                &state.registers[*acc_reg],
                Register::OwnedValue(OwnedValue::Null)
            ) {
                state.registers[*acc_reg] =
                    Register::Aggregate(AggContext::Count(OwnedValue::Integer(0)));
            }
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::Count(count) = agg.borrow_mut() else {
                unreachable!();
            };

            if !(matches!(func, AggFunc::Count) && matches!(col, OwnedValue::Null)) {
                *count += 1;
            };
        }
        AggFunc::Max => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::Max(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            match (acc.as_mut(), col.get_owned_value()) {
                (None, value) => {
                    *acc = Some(value.clone());
                }
                (Some(OwnedValue::Integer(ref mut current_max)), OwnedValue::Integer(value)) => {
                    if *value > *current_max {
                        *current_max = value.clone();
                    }
                }
                (Some(OwnedValue::Float(ref mut current_max)), OwnedValue::Float(value)) => {
                    if *value > *current_max {
                        *current_max = *value;
                    }
                }
                (Some(OwnedValue::Text(ref mut current_max)), OwnedValue::Text(value)) => {
                    if value.value > current_max.value {
                        *current_max = value.clone();
                    }
                }
                _ => {
                    eprintln!("Unexpected types in max aggregation");
                }
            }
        }
        AggFunc::Min => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::Min(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            match (acc.as_mut(), col.get_owned_value()) {
                (None, value) => {
                    *acc.borrow_mut() = Some(value.clone());
                }
                (Some(OwnedValue::Integer(ref mut current_min)), OwnedValue::Integer(value)) => {
                    if *value < *current_min {
                        *current_min = *value;
                    }
                }
                (Some(OwnedValue::Float(ref mut current_min)), OwnedValue::Float(value)) => {
                    if *value < *current_min {
                        *current_min = *value;
                    }
                }
                (Some(OwnedValue::Text(ref mut current_min)), OwnedValue::Text(text)) => {
                    if text.value < current_min.value {
                        *current_min = text.clone();
                    }
                }
                _ => {
                    eprintln!("Unexpected types in min aggregation");
                }
            }
        }
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            let col = state.registers[*col].get_owned_value().clone();
            let delimiter = state.registers[*delimiter].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                unreachable!();
            };
            if acc.to_string().is_empty() {
                *acc = col;
            } else {
                match delimiter {
                    Register::OwnedValue(owned_value) => {
                        *acc += owned_value;
                    }
                    _ => unreachable!(),
                }
                *acc += col;
            }
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            let key = state.registers[*col].clone();
            let value = state.registers[*delimiter].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            let mut key_vec = convert_dbtype_to_raw_jsonb(&key.get_owned_value())?;
            let mut val_vec = convert_dbtype_to_raw_jsonb(&value.get_owned_value())?;

            match acc {
                OwnedValue::Blob(vec) => {
                    if vec.is_empty() {
                        // bits for obj header
                        vec.push(12);
                        vec.append(&mut key_vec);
                        vec.append(&mut val_vec);
                    } else {
                        vec.append(&mut key_vec);
                        vec.append(&mut val_vec);
                    }
                }
                _ => unreachable!(),
            };
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            let col = state.registers[*col].clone();
            let Register::Aggregate(agg) = state.registers[*acc_reg].borrow_mut() else {
                unreachable!();
            };
            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                unreachable!();
            };

            let mut data = convert_dbtype_to_raw_jsonb(&col.get_owned_value())?;
            match acc {
                OwnedValue::Blob(vec) => {
                    if vec.is_empty() {
                        vec.push(11);
                        vec.append(&mut data)
                    } else {
                        vec.append(&mut data);
                    }
                }
                _ => unreachable!(),
            };
        }
        AggFunc::External(_) => {
            let (step_fn, state_ptr, argc) = {
                let Register::Aggregate(agg) = &state.registers[*acc_reg] else {
                    unreachable!();
                };
                let AggContext::External(agg_state) = agg else {
                    unreachable!();
                };
                (agg_state.step_fn, agg_state.state, agg_state.argc)
            };
            if argc == 0 {
                unsafe { step_fn(state_ptr, 0, std::ptr::null()) };
            } else {
                let register_slice = &state.registers[*col..*col + argc];
                let mut ext_values: Vec<ExtValue> = Vec::with_capacity(argc);
                for ov in register_slice.iter() {
                    ext_values.push(ov.get_owned_value().to_ffi());
                }
                let argv_ptr = ext_values.as_ptr();
                unsafe { step_fn(state_ptr, argc as i32, argv_ptr) };
                for ext_value in ext_values {
                    unsafe { ext_value.__free_internal_type() };
                }
            }
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_agg_final(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::AggFinal { register, func } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    match state.registers[*register].borrow_mut() {
        Register::Aggregate(agg) => match func {
            AggFunc::Avg => {
                let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                    unreachable!();
                };
                *acc /= count.clone();
                state.registers[*register] = Register::OwnedValue(acc.clone());
            }
            AggFunc::Sum | AggFunc::Total => {
                let AggContext::Sum(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let value = match acc {
                    OwnedValue::Integer(i) => OwnedValue::Integer(*i),
                    OwnedValue::Float(f) => OwnedValue::Float(*f),
                    _ => OwnedValue::Float(0.0),
                };
                state.registers[*register] = Register::OwnedValue(value);
            }
            AggFunc::Count | AggFunc::Count0 => {
                let AggContext::Count(count) = agg.borrow_mut() else {
                    unreachable!();
                };
                state.registers[*register] = Register::OwnedValue(count.clone());
            }
            AggFunc::Max => {
                let AggContext::Max(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                match acc {
                    Some(value) => state.registers[*register] = Register::OwnedValue(value.clone()),
                    None => state.registers[*register] = Register::OwnedValue(OwnedValue::Null),
                }
            }
            AggFunc::Min => {
                let AggContext::Min(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                match acc {
                    Some(value) => state.registers[*register] = Register::OwnedValue(value.clone()),
                    None => state.registers[*register] = Register::OwnedValue(OwnedValue::Null),
                }
            }
            AggFunc::GroupConcat | AggFunc::StringAgg => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                state.registers[*register] = Register::OwnedValue(acc.clone());
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] =
                    Register::OwnedValue(json_from_raw_bytes_agg(data, false)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonbGroupObject => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] =
                    Register::OwnedValue(json_from_raw_bytes_agg(data, true)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] =
                    Register::OwnedValue(json_from_raw_bytes_agg(data, false)?);
            }
            #[cfg(feature = "json")]
            AggFunc::JsonbGroupArray => {
                let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                    unreachable!();
                };
                let data = acc.to_blob().expect("Should be blob");
                state.registers[*register] =
                    Register::OwnedValue(json_from_raw_bytes_agg(data, true)?);
            }
            AggFunc::External(_) => {
                agg.compute_external()?;
                let AggContext::External(agg_state) = agg else {
                    unreachable!();
                };
                match &agg_state.finalized_value {
                    Some(value) => state.registers[*register] = Register::OwnedValue(value.clone()),
                    None => state.registers[*register] = Register::OwnedValue(OwnedValue::Null),
                }
            }
        },
        Register::OwnedValue(OwnedValue::Null) => {
            // when the set is empty
            match func {
                AggFunc::Total => {
                    state.registers[*register] = Register::OwnedValue(OwnedValue::Float(0.0));
                }
                AggFunc::Count | AggFunc::Count0 => {
                    state.registers[*register] = Register::OwnedValue(OwnedValue::Integer(0));
                }
                _ => {}
            }
        }
        _ => {
            unreachable!();
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_open(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterOpen {
        cursor_id,
        columns: _,
        order,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let order = order
        .get_values()
        .iter()
        .map(|v| match v {
            OwnedValue::Integer(i) => *i == 0,
            _ => unreachable!(),
        })
        .collect();
    let cursor = Sorter::new(order);
    let mut cursors = state.cursors.borrow_mut();
    cursors
        .get_mut(*cursor_id)
        .unwrap()
        .replace(Cursor::new_sorter(cursor));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_data(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterData {
        cursor_id,
        dest_reg,
        pseudo_cursor,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let record = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        cursor.record().map(|r| r.clone())
    };
    let record = match record {
        Some(record) => record,
        None => {
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    };
    state.registers[*dest_reg] = Register::Record(record.clone());
    {
        let mut pseudo_cursor = state.get_cursor(*pseudo_cursor);
        pseudo_cursor.as_pseudo_mut().insert(record);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterInsert {
        cursor_id,
        record_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        let record = match &state.registers[*record_reg] {
            Register::Record(record) => record,
            _ => unreachable!("SorterInsert on non-record register"),
        };
        cursor.insert(record);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_sort(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterSort {
        cursor_id,
        pc_if_empty,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let is_empty = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        let is_empty = cursor.is_empty();
        if !is_empty {
            cursor.sort();
        }
        is_empty
    };
    if is_empty {
        state.pc = pc_if_empty.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_next(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SorterNext {
        cursor_id,
        pc_if_next,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(pc_if_next.is_offset());
    let has_more = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        cursor.next();
        cursor.has_more()
    };
    if has_more {
        state.pc = pc_if_next.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_function(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Function {
        constant_mask,
        func,
        start_reg,
        dest,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let arg_count = func.arg_count;

    match &func.func {
        #[cfg(feature = "json")]
        crate::function::Func::Json(json_func) => match json_func {
            JsonFunc::Json => {
                let json_value = &state.registers[*start_reg];
                let json_str = get_json(json_value.get_owned_value(), None);
                match json_str {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::Jsonb => {
                let json_value = &state.registers[*start_reg];
                let json_blob = jsonb(json_value.get_owned_value(), &state.json_cache);
                match json_blob {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::JsonArray
            | JsonFunc::JsonObject
            | JsonFunc::JsonbArray
            | JsonFunc::JsonbObject => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];

                let json_func = match json_func {
                    JsonFunc::JsonArray => json_array,
                    JsonFunc::JsonObject => json_object,
                    JsonFunc::JsonbArray => jsonb_array,
                    JsonFunc::JsonbObject => jsonb_object,
                    _ => unreachable!(),
                };
                let json_result = json_func(reg_values);

                match json_result {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonExtract => {
                let result = match arg_count {
                    0 => Ok(OwnedValue::Null),
                    _ => {
                        let val = &state.registers[*start_reg];
                        let reg_values = &state.registers[*start_reg + 1..*start_reg + arg_count];

                        json_extract(val.get_owned_value(), reg_values, &state.json_cache)
                    }
                };

                match result {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonbExtract => {
                let result = match arg_count {
                    0 => Ok(OwnedValue::Null),
                    _ => {
                        let val = &state.registers[*start_reg];
                        let reg_values = &state.registers[*start_reg + 1..*start_reg + arg_count];

                        jsonb_extract(val.get_owned_value(), reg_values, &state.json_cache)
                    }
                };

                match result {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }

            JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                assert_eq!(arg_count, 2);
                let json = &state.registers[*start_reg];
                let path = &state.registers[*start_reg + 1];
                let json_func = match json_func {
                    JsonFunc::JsonArrowExtract => json_arrow_extract,
                    JsonFunc::JsonArrowShiftExtract => json_arrow_shift_extract,
                    _ => unreachable!(),
                };
                let json_str = json_func(
                    json.get_owned_value(),
                    path.get_owned_value(),
                    &state.json_cache,
                );
                match json_str {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                let json_value = &state.registers[*start_reg];
                let path_value = if arg_count > 1 {
                    Some(&state.registers[*start_reg + 1])
                } else {
                    None
                };
                let func_result = match json_func {
                    JsonFunc::JsonArrayLength => json_array_length(
                        json_value.get_owned_value(),
                        path_value.map(|x| x.get_owned_value()),
                        &state.json_cache,
                    ),
                    JsonFunc::JsonType => json_type(
                        json_value.get_owned_value(),
                        path_value.map(|x| x.get_owned_value()),
                    ),
                    _ => unreachable!(),
                };

                match func_result {
                    Ok(result) => state.registers[*dest] = Register::OwnedValue(result),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonErrorPosition => {
                let json_value = &state.registers[*start_reg];
                match json_error_position(json_value.get_owned_value()) {
                    Ok(pos) => state.registers[*dest] = Register::OwnedValue(pos),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonValid => {
                let json_value = &state.registers[*start_reg];
                state.registers[*dest] =
                    Register::OwnedValue(is_json_valid(json_value.get_owned_value()));
            }
            JsonFunc::JsonPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::OwnedValue(json_patch(
                    target.get_owned_value(),
                    patch.get_owned_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonbPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::OwnedValue(jsonb_patch(
                    target.get_owned_value(),
                    patch.get_owned_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonRemove => {
                if let Ok(json) = json_remove(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::OwnedValue(json);
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            JsonFunc::JsonbRemove => {
                if let Ok(json) = jsonb_remove(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::OwnedValue(json);
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            JsonFunc::JsonReplace => {
                if let Ok(json) = json_replace(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::OwnedValue(json);
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            JsonFunc::JsonbReplace => {
                if let Ok(json) = jsonb_replace(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::OwnedValue(json);
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            JsonFunc::JsonInsert => {
                if let Ok(json) = json_insert(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::OwnedValue(json);
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            JsonFunc::JsonbInsert => {
                if let Ok(json) = jsonb_insert(
                    &state.registers[*start_reg..*start_reg + arg_count],
                    &state.json_cache,
                ) {
                    state.registers[*dest] = Register::OwnedValue(json);
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            JsonFunc::JsonPretty => {
                let json_value = &state.registers[*start_reg];
                let indent = if arg_count > 1 {
                    Some(&state.registers[*start_reg + 1])
                } else {
                    None
                };

                // Blob should be converted to Ascii in a lossy way
                // However, Rust strings uses utf-8
                // so the behavior at the moment is slightly different
                // To the way blobs are parsed here in SQLite.
                let indent = match indent {
                    Some(value) => match value.get_owned_value() {
                        OwnedValue::Text(text) => text.as_str(),
                        OwnedValue::Integer(val) => &val.to_string(),
                        OwnedValue::Float(val) => &val.to_string(),
                        OwnedValue::Blob(val) => &String::from_utf8_lossy(val),
                        _ => "    ",
                    },
                    // If the second argument is omitted or is NULL, then indentation is four spaces per level
                    None => "    ",
                };

                let json_str = get_json(json_value.get_owned_value(), Some(indent))?;
                state.registers[*dest] = Register::OwnedValue(json_str);
            }
            JsonFunc::JsonSet => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_set() needs an odd number of arguments")
                }
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];

                let json_result = json_set(reg_values, &state.json_cache);

                match json_result {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonbSet => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_set() needs an odd number of arguments")
                }
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];

                let json_result = jsonb_set(reg_values, &state.json_cache);

                match json_result {
                    Ok(json) => state.registers[*dest] = Register::OwnedValue(json),
                    Err(e) => return Err(e),
                }
            }
            JsonFunc::JsonQuote => {
                let json_value = &state.registers[*start_reg];

                match json_quote(json_value.get_owned_value()) {
                    Ok(result) => state.registers[*dest] = Register::OwnedValue(result),
                    Err(e) => return Err(e),
                }
            }
        },
        crate::function::Func::Scalar(scalar_func) => match scalar_func {
            ScalarFunc::Cast => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let reg_value_argument = state.registers[*start_reg].clone();
                let OwnedValue::Text(reg_value_type) =
                    state.registers[*start_reg + 1].get_owned_value().clone()
                else {
                    unreachable!("Cast with non-text type");
                };
                let result = exec_cast(
                    &reg_value_argument.get_owned_value(),
                    reg_value_type.as_str(),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Changes => {
                let res = &program.connection.upgrade().unwrap().last_change;
                let changes = res.get();
                state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(changes));
            }
            ScalarFunc::Char => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::OwnedValue(exec_char(reg_values));
            }
            ScalarFunc::Coalesce => {}
            ScalarFunc::Concat => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = exec_concat_strings(reg_values);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::ConcatWs => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = exec_concat_ws(reg_values);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Glob => {
                let pattern = &state.registers[*start_reg];
                let text = &state.registers[*start_reg + 1];
                let result = match (pattern.get_owned_value(), text.get_owned_value()) {
                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                        let cache = if *constant_mask > 0 {
                            Some(&mut state.regex_cache.glob)
                        } else {
                            None
                        };
                        OwnedValue::Integer(exec_glob(cache, pattern.as_str(), text.as_str()) as i64)
                    }
                    _ => {
                        unreachable!("Like on non-text registers");
                    }
                };
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::IfNull => {}
            ScalarFunc::Iif => {}
            ScalarFunc::Instr => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = &state.registers[*start_reg + 1];
                let result =
                    exec_instr(reg_value.get_owned_value(), pattern_value.get_owned_value());
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::LastInsertRowid => {
                if let Some(conn) = program.connection.upgrade() {
                    state.registers[*dest] =
                        Register::OwnedValue(OwnedValue::Integer(conn.last_insert_rowid() as i64));
                } else {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                }
            }
            ScalarFunc::Like => {
                let pattern = &state.registers[*start_reg];
                let match_expression = &state.registers[*start_reg + 1];

                let pattern = match pattern.get_owned_value() {
                    OwnedValue::Text(_) => pattern.get_owned_value(),
                    _ => &exec_cast(pattern.get_owned_value(), "TEXT"),
                };
                let match_expression = match match_expression.get_owned_value() {
                    OwnedValue::Text(_) => match_expression.get_owned_value(),
                    _ => &exec_cast(match_expression.get_owned_value(), "TEXT"),
                };

                let result = match (pattern, match_expression) {
                    (OwnedValue::Text(pattern), OwnedValue::Text(match_expression))
                        if arg_count == 3 =>
                    {
                        let escape = match construct_like_escape_arg(
                            state.registers[*start_reg + 2].get_owned_value(),
                        ) {
                            Ok(x) => x,
                            Err(e) => return Err(e),
                        };

                        OwnedValue::Integer(exec_like_with_escape(
                            pattern.as_str(),
                            match_expression.as_str(),
                            escape,
                        ) as i64)
                    }
                    (OwnedValue::Text(pattern), OwnedValue::Text(match_expression)) => {
                        let cache = if *constant_mask > 0 {
                            Some(&mut state.regex_cache.like)
                        } else {
                            None
                        };
                        OwnedValue::Integer(exec_like(
                            cache,
                            pattern.as_str(),
                            match_expression.as_str(),
                        ) as i64)
                    }
                    (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
                    _ => {
                        unreachable!("Like failed");
                    }
                };

                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Abs
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob => {
                let reg_value = state.registers[*start_reg].borrow_mut().get_owned_value();
                let result = match scalar_func {
                    ScalarFunc::Sign => exec_sign(reg_value),
                    ScalarFunc::Abs => Some(exec_abs(reg_value)?),
                    ScalarFunc::Lower => exec_lower(reg_value),
                    ScalarFunc::Upper => exec_upper(reg_value),
                    ScalarFunc::Length => Some(exec_length(reg_value)),
                    ScalarFunc::OctetLength => Some(exec_octet_length(reg_value)),
                    ScalarFunc::Typeof => Some(exec_typeof(reg_value)),
                    ScalarFunc::Unicode => Some(exec_unicode(reg_value)),
                    ScalarFunc::Quote => Some(exec_quote(reg_value)),
                    ScalarFunc::RandomBlob => Some(exec_randomblob(reg_value)),
                    ScalarFunc::ZeroBlob => Some(exec_zeroblob(reg_value)),
                    ScalarFunc::Soundex => Some(exec_soundex(reg_value)),
                    _ => unreachable!(),
                };
                state.registers[*dest] = Register::OwnedValue(result.unwrap_or(OwnedValue::Null));
            }
            ScalarFunc::Hex => {
                let reg_value = state.registers[*start_reg].borrow_mut();
                let result = exec_hex(reg_value.get_owned_value());
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Unhex => {
                let reg_value = &state.registers[*start_reg];
                let ignored_chars = state.registers.get(*start_reg + 1);
                let result = exec_unhex(
                    reg_value.get_owned_value(),
                    ignored_chars.map(|x| x.get_owned_value()),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Random => {
                state.registers[*dest] = Register::OwnedValue(exec_random());
            }
            ScalarFunc::Trim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = exec_trim(
                    reg_value.get_owned_value(),
                    pattern_value.map(|x| x.get_owned_value()),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::LTrim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = exec_ltrim(
                    reg_value.get_owned_value(),
                    pattern_value.map(|x| x.get_owned_value()),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::RTrim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = exec_rtrim(
                    &reg_value.get_owned_value(),
                    pattern_value.map(|x| x.get_owned_value()),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Round => {
                let reg_value = &state.registers[*start_reg];
                assert!(arg_count == 1 || arg_count == 2);
                let precision_value = if arg_count > 1 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = exec_round(
                    reg_value.get_owned_value(),
                    precision_value.map(|x| x.get_owned_value()),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Min => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::OwnedValue(exec_min(reg_values));
            }
            ScalarFunc::Max => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest] = Register::OwnedValue(exec_max(reg_values));
            }
            ScalarFunc::Nullif => {
                let first_value = &state.registers[*start_reg];
                let second_value = &state.registers[*start_reg + 1];
                state.registers[*dest] = Register::OwnedValue(exec_nullif(
                    first_value.get_owned_value(),
                    second_value.get_owned_value(),
                ));
            }
            ScalarFunc::Substr | ScalarFunc::Substring => {
                let str_value = &state.registers[*start_reg];
                let start_value = &state.registers[*start_reg + 1];
                let length_value = if func.arg_count == 3 {
                    Some(&state.registers[*start_reg + 2])
                } else {
                    None
                };
                let result = exec_substring(
                    str_value.get_owned_value(),
                    start_value.get_owned_value(),
                    length_value.map(|x| x.get_owned_value()),
                );
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Date => {
                let result = exec_date(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Time => {
                let values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = exec_time(values);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::TimeDiff => {
                if arg_count != 2 {
                    state.registers[*dest] = Register::OwnedValue(OwnedValue::Null);
                } else {
                    let start = state.registers[*start_reg].get_owned_value().clone();
                    let end = state.registers[*start_reg + 1].get_owned_value().clone();

                    let result = crate::functions::datetime::exec_timediff(&[
                        Register::OwnedValue(start),
                        Register::OwnedValue(end),
                    ]);

                    state.registers[*dest] = Register::OwnedValue(result);
                }
            }
            ScalarFunc::TotalChanges => {
                let res = &program.connection.upgrade().unwrap().total_changes;
                let total_changes = res.get();
                state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(total_changes));
            }
            ScalarFunc::DateTime => {
                let result =
                    exec_datetime_full(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::JulianDay => {
                let result = exec_julianday(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::UnixEpoch => {
                if *start_reg == 0 {
                    let unixepoch: String = exec_unixepoch(&OwnedValue::build_text("now"))?;
                    state.registers[*dest] =
                        Register::OwnedValue(OwnedValue::build_text(&unixepoch));
                } else {
                    let datetime_value = &state.registers[*start_reg];
                    let unixepoch = exec_unixepoch(datetime_value.get_owned_value());
                    match unixepoch {
                        Ok(time) => {
                            state.registers[*dest] =
                                Register::OwnedValue(OwnedValue::build_text(&time))
                        }
                        Err(e) => {
                            return Err(LimboError::ParseError(format!(
                                "Error encountered while parsing datetime value: {}",
                                e
                            )));
                        }
                    }
                }
            }
            ScalarFunc::SqliteVersion => {
                let version_integer: i64 = DATABASE_VERSION.get().unwrap().parse()?;
                let version = execute_sqlite_version(version_integer);
                state.registers[*dest] = Register::OwnedValue(OwnedValue::build_text(&version));
            }
            ScalarFunc::SqliteSourceId => {
                let src_id = format!(
                    "{} {}",
                    info::build::BUILT_TIME_SQLITE,
                    info::build::GIT_COMMIT_HASH.unwrap_or("unknown")
                );
                state.registers[*dest] = Register::OwnedValue(OwnedValue::build_text(&src_id));
            }
            ScalarFunc::Replace => {
                assert_eq!(arg_count, 3);
                let source = &state.registers[*start_reg];
                let pattern = &state.registers[*start_reg + 1];
                let replacement = &state.registers[*start_reg + 2];
                state.registers[*dest] = Register::OwnedValue(exec_replace(
                    source.get_owned_value(),
                    pattern.get_owned_value(),
                    replacement.get_owned_value(),
                ));
            }
            #[cfg(feature = "fs")]
            ScalarFunc::LoadExtension => {
                let extension = &state.registers[*start_reg];
                let ext = resolve_ext_path(&extension.get_owned_value().to_string())?;
                if let Some(conn) = program.connection.upgrade() {
                    conn.load_extension(ext)?;
                }
            }
            ScalarFunc::StrfTime => {
                let result = exec_strftime(&state.registers[*start_reg..*start_reg + arg_count]);
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Printf => {
                let result = exec_printf(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Likely => {
                let value = &state.registers[*start_reg].borrow_mut();
                let result = exec_likely(value.get_owned_value());
                state.registers[*dest] = Register::OwnedValue(result);
            }
            ScalarFunc::Likelihood => {
                assert_eq!(arg_count, 2);
                let value = &state.registers[*start_reg];
                let probability = &state.registers[*start_reg + 1];
                let result =
                    exec_likelihood(value.get_owned_value(), probability.get_owned_value());
                state.registers[*dest] = Register::OwnedValue(result);
            }
        },
        crate::function::Func::Vector(vector_func) => match vector_func {
            VectorFunc::Vector => {
                let result = vector32(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::OwnedValue(result);
            }
            VectorFunc::Vector32 => {
                let result = vector32(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::OwnedValue(result);
            }
            VectorFunc::Vector64 => {
                let result = vector64(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::OwnedValue(result);
            }
            VectorFunc::VectorExtract => {
                let result = vector_extract(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::OwnedValue(result);
            }
            VectorFunc::VectorDistanceCos => {
                let result =
                    vector_distance_cos(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest] = Register::OwnedValue(result);
            }
        },
        crate::function::Func::External(f) => match f.func {
            ExtFunc::Scalar(f) => {
                if arg_count == 0 {
                    let result_c_value: ExtValue = unsafe { (f)(0, std::ptr::null()) };
                    match OwnedValue::from_ffi(result_c_value) {
                        Ok(result_ov) => {
                            state.registers[*dest] = Register::OwnedValue(result_ov);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                } else {
                    let register_slice = &state.registers[*start_reg..*start_reg + arg_count];
                    let mut ext_values: Vec<ExtValue> = Vec::with_capacity(arg_count);
                    for ov in register_slice.iter() {
                        let val = ov.get_owned_value().to_ffi();
                        ext_values.push(val);
                    }
                    let argv_ptr = ext_values.as_ptr();
                    let result_c_value: ExtValue = unsafe { (f)(arg_count as i32, argv_ptr) };
                    match OwnedValue::from_ffi(result_c_value) {
                        Ok(result_ov) => {
                            state.registers[*dest] = Register::OwnedValue(result_ov);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }
            _ => unreachable!("aggregate called in scalar context"),
        },
        crate::function::Func::Math(math_func) => match math_func.arity() {
            MathFuncArity::Nullary => match math_func {
                MathFunc::Pi => {
                    state.registers[*dest] =
                        Register::OwnedValue(OwnedValue::Float(std::f64::consts::PI));
                }
                _ => {
                    unreachable!("Unexpected mathematical Nullary function {:?}", math_func);
                }
            },

            MathFuncArity::Unary => {
                let reg_value = &state.registers[*start_reg];
                let result = exec_math_unary(reg_value.get_owned_value(), math_func);
                state.registers[*dest] = Register::OwnedValue(result);
            }

            MathFuncArity::Binary => {
                let lhs = &state.registers[*start_reg];
                let rhs = &state.registers[*start_reg + 1];
                let result =
                    exec_math_binary(lhs.get_owned_value(), rhs.get_owned_value(), math_func);
                state.registers[*dest] = Register::OwnedValue(result);
            }

            MathFuncArity::UnaryOrBinary => match math_func {
                MathFunc::Log => {
                    let result = match arg_count {
                        1 => {
                            let arg = &state.registers[*start_reg];
                            exec_math_log(arg.get_owned_value(), None)
                        }
                        2 => {
                            let base = &state.registers[*start_reg];
                            let arg = &state.registers[*start_reg + 1];
                            exec_math_log(arg.get_owned_value(), Some(base.get_owned_value()))
                        }
                        _ => unreachable!(
                            "{:?} function with unexpected number of arguments",
                            math_func
                        ),
                    };
                    state.registers[*dest] = Register::OwnedValue(result);
                }
                _ => unreachable!(
                    "Unexpected mathematical UnaryOrBinary function {:?}",
                    math_func
                ),
            },
        },
        crate::function::Func::Agg(_) => {
            unreachable!("Aggregate functions should not be handled here")
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_init_coroutine(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::InitCoroutine {
        yield_reg,
        jump_on_definition,
        start_offset,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    assert!(jump_on_definition.is_offset());
    let start_offset = start_offset.to_offset_int();
    state.registers[*yield_reg] = Register::OwnedValue(OwnedValue::Integer(start_offset as i64));
    state.ended_coroutine.unset(*yield_reg);
    let jump_on_definition = jump_on_definition.to_offset_int();
    state.pc = if jump_on_definition == 0 {
        state.pc + 1
    } else {
        jump_on_definition
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_end_coroutine(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::EndCoroutine { yield_reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let OwnedValue::Integer(pc) = state.registers[*yield_reg].get_owned_value() {
        state.ended_coroutine.set(*yield_reg);
        let pc: u32 = (*pc)
            .try_into()
            .unwrap_or_else(|_| panic!("EndCoroutine: pc overflow: {}", pc));
        state.pc = pc - 1; // yield jump is always next to yield. Here we subtract 1 to go back to yield instruction
    } else {
        unreachable!();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_yield(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Yield {
        yield_reg,
        end_offset,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if let OwnedValue::Integer(pc) = state.registers[*yield_reg].get_owned_value() {
        if state.ended_coroutine.get(*yield_reg) {
            state.pc = end_offset.to_offset_int();
        } else {
            let pc: u32 = (*pc)
                .try_into()
                .unwrap_or_else(|_| panic!("Yield: pc overflow: {}", pc));
            // swap the program counter with the value in the yield register
            // this is the mechanism that allows jumping back and forth between the coroutine and the caller
            (state.pc, state.registers[*yield_reg]) = (
                pc,
                Register::OwnedValue(OwnedValue::Integer((state.pc + 1) as i64)),
            );
        }
    } else {
        unreachable!(
            "yield_reg {} contains non-integer value: {:?}",
            *yield_reg, state.registers[*yield_reg]
        );
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Insert {
        cursor,
        key_reg,
        record_reg,
        flag: _,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = state.get_cursor(*cursor);
        let cursor = cursor.as_btree_mut();
        let record = match &state.registers[*record_reg] {
            Register::Record(r) => r,
            _ => unreachable!("Not a record! Cannot insert a non record value."),
        };
        let key = match &state.registers[*key_reg].get_owned_value() {
            OwnedValue::Integer(i) => *i,
            _ => unreachable!("expected integer key"),
        };
        // NOTE(pere): Sending moved_before == true is okay because we moved before but
        // if we were to set to false after starting a balance procedure, it might
        // leave undefined state.
        return_if_io!(cursor.insert(&BTreeKey::new_table_rowid(key as u64, Some(record)), true));
        // Only update last_insert_rowid for regular table inserts, not schema modifications
        if cursor.root_page() != 1 {
            if let Some(rowid) = cursor.rowid()? {
                if let Some(conn) = program.connection.upgrade() {
                    conn.update_last_rowid(rowid);
                }
                let prev_changes = program.n_change.get();
                program.n_change.set(prev_changes + 1);
            }
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_delete(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Delete { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.delete());
    }
    let prev_changes = program.n_change.get();
    program.n_change.set(prev_changes + 1);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    if let Insn::IdxInsert {
        cursor_id,
        record_reg,
        flags,
        ..
    } = *insn
    {
        let (_, cursor_type) = program.cursor_ref.get(cursor_id).unwrap();
        let CursorType::BTreeIndex(index_meta) = cursor_type else {
            panic!("IdxInsert: not a BTree index cursor");
        };
        {
            let mut cursor = state.get_cursor(cursor_id);
            let cursor = cursor.as_btree_mut();
            let record = match &state.registers[record_reg] {
                Register::Record(ref r) => r,
                _ => return Err(LimboError::InternalError("expected record".into())),
            };
            // To make this reentrant in case of `moved_before` = false, we need to check if the previous cursor.insert started
            // a write/balancing operation. If it did, it means we already moved to the place we wanted.
            let moved_before = if cursor.is_write_in_progress() {
                true
            } else {
                if index_meta.unique {
                    // check for uniqueness violation
                    match cursor.key_exists_in_index(record)? {
                        CursorResult::Ok(true) => {
                            return Err(LimboError::Constraint(
                                "UNIQUE constraint failed: duplicate key".into(),
                            ))
                        }
                        CursorResult::IO => return Ok(InsnFunctionStepResult::IO),
                        CursorResult::Ok(false) => {}
                    };
                    false
                } else {
                    flags.has(IdxInsertFlags::USE_SEEK)
                }
            };

            // Start insertion of row. This might trigger a balance procedure which will take care of moving to different pages,
            // therefore, we don't want to seek again if that happens, meaning we don't want to return on io without moving to the following opcode
            // because it could trigger a movement to child page after a balance root which will leave the current page as the root page.
            return_if_io!(cursor.insert(&BTreeKey::new_index_key(record), moved_before));
        }
        // TODO: flag optimizations, update n_change if OPFLAG_NCHANGE
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_new_rowid(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NewRowid {
        cursor, rowid_reg, ..
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let rowid = {
        let mut cursor = state.get_cursor(*cursor);
        let cursor = cursor.as_btree_mut();
        // TODO: make io handle rng
        let rowid = return_if_io!(get_new_rowid(cursor, thread_rng()));
        rowid
    };
    state.registers[*rowid_reg] = Register::OwnedValue(OwnedValue::Integer(rowid));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_must_be_int(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::MustBeInt { reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    match &state.registers[*reg].get_owned_value() {
        OwnedValue::Integer(_) => {}
        OwnedValue::Float(f) => match cast_real_to_integer(*f) {
            Ok(i) => state.registers[*reg] = Register::OwnedValue(OwnedValue::Integer(i)),
            Err(_) => crate::bail_parse_error!(
                "MustBeInt: the value in register cannot be cast to integer"
            ),
        },
        OwnedValue::Text(text) => match checked_cast_text_to_numeric(text.as_str()) {
            Ok(OwnedValue::Integer(i)) => {
                state.registers[*reg] = Register::OwnedValue(OwnedValue::Integer(i))
            }
            Ok(OwnedValue::Float(f)) => {
                state.registers[*reg] = Register::OwnedValue(OwnedValue::Integer(f as i64))
            }
            _ => crate::bail_parse_error!(
                "MustBeInt: the value in register cannot be cast to integer"
            ),
        },
        _ => {
            crate::bail_parse_error!("MustBeInt: the value in register cannot be cast to integer");
        }
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_soft_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::SoftNull { reg } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*reg] = Register::OwnedValue(OwnedValue::Null);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not_exists(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NotExists {
        cursor,
        rowid_reg,
        target_pc,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let exists = {
        let mut cursor = must_be_btree_cursor!(*cursor, program.cursor_ref, state, "NotExists");
        let cursor = cursor.as_btree_mut();
        let exists = return_if_io!(cursor.exists(state.registers[*rowid_reg].get_owned_value()));
        exists
    };
    if exists {
        state.pc += 1;
    } else {
        state.pc = target_pc.to_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_offset_limit(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OffsetLimit {
        limit_reg,
        combined_reg,
        offset_reg,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let limit_val = match state.registers[*limit_reg].get_owned_value() {
        OwnedValue::Integer(val) => val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in limit_reg is not an integer".into(),
            ));
        }
    };
    let offset_val = match state.registers[*offset_reg].get_owned_value() {
        OwnedValue::Integer(val) if *val < 0 => 0,
        OwnedValue::Integer(val) if *val >= 0 => *val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in offset_reg is not an integer".into(),
            ));
        }
    };

    let offset_limit_sum = limit_val.overflowing_add(offset_val);
    if *limit_val <= 0 || offset_limit_sum.1 {
        state.registers[*combined_reg] = Register::OwnedValue(OwnedValue::Integer(-1));
    } else {
        state.registers[*combined_reg] =
            Register::OwnedValue(OwnedValue::Integer(offset_limit_sum.0));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}
// this cursor may be reused for next insert
// Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
// If not there might be some optimizations obviously.
pub fn op_open_write(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::OpenWrite {
        cursor_id,
        root_page,
        ..
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let root_page = match root_page {
        RegisterOrLiteral::Literal(lit) => *lit as u64,
        RegisterOrLiteral::Register(reg) => match &state.registers[*reg].get_owned_value() {
            OwnedValue::Integer(val) => *val as u64,
            _ => {
                return Err(LimboError::InternalError(
                    "OpenWrite: the value in root_page is not an integer".into(),
                ));
            }
        },
    };
    let (_, cursor_type) = program.cursor_ref.get(*cursor_id).unwrap();
    let mut cursors = state.cursors.borrow_mut();
    let maybe_index = match cursor_type {
        CursorType::BTreeIndex(index) => Some(index),
        _ => None,
    };
    let mv_cursor = match state.mv_tx_id {
        Some(tx_id) => {
            let table_id = root_page;
            let mv_store = mv_store.unwrap().clone();
            let mv_cursor = Rc::new(RefCell::new(
                MvCursor::new(mv_store.clone(), tx_id, table_id).unwrap(),
            ));
            Some(mv_cursor)
        }
        None => None,
    };
    if let Some(index) = maybe_index {
        let cursor =
            BTreeCursor::new_index(mv_cursor, pager.clone(), root_page as usize, index.as_ref());
        cursors
            .get_mut(*cursor_id)
            .unwrap()
            .replace(Cursor::new_btree(cursor));
    } else {
        let cursor = BTreeCursor::new(mv_cursor, pager.clone(), root_page as usize);
        cursors
            .get_mut(*cursor_id)
            .unwrap()
            .replace(Cursor::new_btree(cursor));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_copy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Copy {
        src_reg,
        dst_reg,
        amount,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    for i in 0..=*amount {
        state.registers[*dst_reg + i] = state.registers[*src_reg + i].clone();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_create_btree(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::CreateBtree { db, root, flags } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }
    let root_page = pager.btree_create(flags);
    state.registers[*root] = Register::OwnedValue(OwnedValue::Integer(root_page as i64));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_destroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Destroy {
        root,
        former_root_reg: _,
        is_temp,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *is_temp == 1 {
        todo!("temp databases not implemented yet.");
    }
    let mut cursor = BTreeCursor::new(None, pager.clone(), *root);
    cursor.btree_destroy()?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::DropTable {
        db,
        _p2,
        _p3,
        table_name,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        todo!("temp databases not implemented yet");
    }
    if let Some(conn) = program.connection.upgrade() {
        let mut schema = conn.schema.write();
        schema.remove_indices_for_table(table_name);
        schema.remove_table(table_name);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_close(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Close { cursor_id } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let mut cursors = state.cursors.borrow_mut();
    cursors.get_mut(*cursor_id).unwrap().take();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_is_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::IsNull { reg, target_pc } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if matches!(
        state.registers[*reg],
        Register::OwnedValue(OwnedValue::Null)
    ) {
        state.pc = target_pc.to_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_page_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::PageCount { db, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }
    // SQLite returns "0" on an empty database, and 2 on the first insertion,
    // so we'll mimic that behavior.
    let mut pages = pager.db_header.lock().database_size.into();
    if pages == 1 {
        pages = 0;
    }
    state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(pages));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_parse_schema(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ParseSchema {
        db: _,
        where_clause,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    let conn = program.connection.upgrade();
    let conn = conn.as_ref().unwrap();
    let stmt = conn.prepare(format!(
        "SELECT * FROM sqlite_schema WHERE {}",
        where_clause
    ))?;
    let mut schema = conn.schema.write();
    // TODO: This function below is synchronous, make it async
    {
        parse_schema_rows(
            Some(stmt),
            &mut schema,
            conn.pager.io.clone(),
            &conn.syms.borrow(),
            state.mv_tx_id,
        )?;
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_read_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ReadCookie { db, dest, cookie } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *db > 0 {
        // TODO: implement temp databases
        todo!("temp databases not implemented yet");
    }
    let cookie_value = match cookie {
        Cookie::UserVersion => pager.db_header.lock().user_version.into(),
        cookie => todo!("{cookie:?} is not yet implement for ReadCookie"),
    };
    state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(cookie_value));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_right(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ShiftRight { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_shift_right(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_left(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ShiftLeft { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_shift_left(
        state.registers[*lhs].get_owned_value(),
        state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_variable(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Variable { index, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(state.get_parameter(*index));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_zero_or_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::ZeroOrNull { rg1, rg2, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    if *state.registers[*rg1].get_owned_value() == OwnedValue::Null
        || *state.registers[*rg2].get_owned_value() == OwnedValue::Null
    {
        state.registers[*dest] = Register::OwnedValue(OwnedValue::Null)
    } else {
        state.registers[*dest] = Register::OwnedValue(OwnedValue::Integer(0));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Not { reg, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] =
        Register::OwnedValue(exec_boolean_not(state.registers[*reg].get_owned_value()));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_concat(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Concat { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_concat(
        &state.registers[*lhs].get_owned_value(),
        &state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_and(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::And { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_and(
        &state.registers[*lhs].get_owned_value(),
        &state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_or(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Or { lhs, rhs, dest } = insn else {
        unreachable!("unexpected Insn {:?}", insn)
    };
    state.registers[*dest] = Register::OwnedValue(exec_or(
        &state.registers[*lhs].get_owned_value(),
        &state.registers[*rhs].get_owned_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_noop(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    // Do nothing
    // Advance the program counter for the next opcode
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_ephemeral(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let (cursor_id, is_table) = match insn {
        Insn::OpenEphemeral {
            cursor_id,
            is_table,
        } => (*cursor_id, *is_table),
        Insn::OpenAutoindex { cursor_id } => (*cursor_id, false),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };

    let conn = program.connection.upgrade().unwrap();
    let io = conn.pager.io.get_memory_io();

    let file = io.open_file("", OpenFlags::Create, true)?;
    maybe_init_database_file(&file, &(io.clone() as Arc<dyn IO>))?;
    let db_file = Arc::new(FileMemoryStorage::new(file));

    let db_header = Pager::begin_open(db_file.clone())?;
    let buffer_pool = Rc::new(BufferPool::new(db_header.lock().page_size as usize));
    let page_cache = Arc::new(RwLock::new(DumbLruPageCache::new(10)));

    let pager = Rc::new(Pager::finish_open(
        db_header,
        db_file,
        None,
        io,
        page_cache,
        buffer_pool,
    )?);

    let flag = if is_table {
        &CreateBTreeFlags::new_table()
    } else {
        &CreateBTreeFlags::new_index()
    };

    let root_page = pager.btree_create(flag);

    let (_, cursor_type) = program.cursor_ref.get(cursor_id).unwrap();
    let mv_cursor = match state.mv_tx_id {
        Some(tx_id) => {
            let table_id = root_page as u64;
            let mv_store = mv_store.unwrap().clone();
            let mv_cursor = Rc::new(RefCell::new(
                MvCursor::new(mv_store.clone(), tx_id, table_id).unwrap(),
            ));
            Some(mv_cursor)
        }
        None => None,
    };
    let mut cursor = BTreeCursor::new(mv_cursor, pager, root_page as usize);
    cursor.rewind()?; // Will never return io

    let mut cursors: std::cell::RefMut<'_, Vec<Option<Cursor>>> = state.cursors.borrow_mut();

    // Table content is erased if the cursor already exists
    match cursor_type {
        CursorType::BTreeTable(_) => {
            cursors
                .get_mut(cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(_) => {
            cursors
                .get_mut(cursor_id)
                .unwrap()
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::Pseudo(_) => {
            panic!("OpenEphemeral on pseudo cursor");
        }
        CursorType::Sorter => {
            panic!("OpenEphemeral on sorter cursor");
        }
        CursorType::VirtualTable(_) => {
            panic!("OpenEphemeral on virtual table cursor, use Insn::VOpen instead");
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Execute the [Insn::Once] instruction.
///
/// This instruction is used to execute a block of code only once.
/// If the instruction is executed again, it will jump to the target program counter.
pub fn op_once(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Once {
        target_pc_when_reentered,
    } = insn
    else {
        unreachable!("unexpected Insn: {:?}", insn)
    };
    assert!(target_pc_when_reentered.is_offset());
    let offset = state.pc;
    if state.once.iter().any(|o| o == offset) {
        state.pc = target_pc_when_reentered.to_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }
    state.once.push(offset);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not_found(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::NotFound {
        cursor_id,
        target_pc,
        record_reg,
        num_regs,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    let found = {
        let mut cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_btree_mut();

        if *num_regs == 0 {
            let record = match &state.registers[*record_reg] {
                Register::Record(r) => r,
                _ => {
                    return Err(LimboError::InternalError(
                        "NotFound: exepected a record in the register".into(),
                    ));
                }
            };

            return_if_io!(cursor.seek(SeekKey::IndexKey(&record), SeekOp::EQ))
        } else {
            let record = make_record(&state.registers, record_reg, num_regs);
            return_if_io!(cursor.seek(SeekKey::IndexKey(&record), SeekOp::EQ))
        }
    };

    if found {
        state.pc += 1;
    } else {
        state.pc = target_pc.to_offset_int();
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_affinity(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Rc<Pager>,
    mv_store: Option<&Rc<MvStore>>,
) -> Result<InsnFunctionStepResult> {
    let Insn::Affinity {
        start_reg,
        count,
        affinities,
    } = insn
    else {
        unreachable!("unexpected Insn {:?}", insn)
    };

    if affinities.len() != count.get() {
        return Err(LimboError::InternalError(
            "Affinity: the length of affinities does not match the count".into(),
        ));
    }

    for (i, affinity_char) in affinities.chars().enumerate().take(count.get()) {
        let reg_index = *start_reg + i;

        let affinity = Affinity::from_char(affinity_char)?;

        apply_affinity_char(&mut state.registers[reg_index], affinity);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

fn exec_lower(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::build_text(&t.as_str().to_lowercase())),
        t => Some(t.to_owned()),
    }
}

fn exec_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().chars().count() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        _ => reg.to_owned(),
    }
}

fn exec_octet_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().into_bytes().len() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        _ => reg.to_owned(),
    }
}

fn exec_upper(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::build_text(&t.as_str().to_uppercase())),
        t => Some(t.to_owned()),
    }
}

fn exec_concat_strings(registers: &[Register]) -> OwnedValue {
    let mut result = String::new();
    for reg in registers {
        match reg.get_owned_value() {
            OwnedValue::Null => continue,
            OwnedValue::Blob(_) => todo!("TODO concat blob"),
            v => result.push_str(&format!("{}", v)),
        }
    }
    OwnedValue::build_text(&result)
}

fn exec_concat_ws(registers: &[Register]) -> OwnedValue {
    if registers.is_empty() {
        return OwnedValue::Null;
    }

    let separator = match &registers[0].get_owned_value() {
        OwnedValue::Null | OwnedValue::Blob(_) => return OwnedValue::Null,
        v => format!("{}", v),
    };

    let mut result = String::new();
    for (i, reg) in registers.iter().enumerate().skip(1) {
        if i > 1 {
            result.push_str(&separator);
        }
        match reg.get_owned_value() {
            v if matches!(
                v,
                OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_)
            ) =>
            {
                result.push_str(&format!("{}", v))
            }
            _ => continue,
        }
    }

    OwnedValue::build_text(&result)
}

fn exec_sign(reg: &OwnedValue) -> Option<OwnedValue> {
    let num = match reg {
        OwnedValue::Integer(i) => *i as f64,
        OwnedValue::Float(f) => *f,
        OwnedValue::Text(s) => {
            if let Ok(i) = s.as_str().parse::<i64>() {
                i as f64
            } else if let Ok(f) = s.as_str().parse::<f64>() {
                f
            } else {
                return Some(OwnedValue::Null);
            }
        }
        OwnedValue::Blob(b) => match std::str::from_utf8(b) {
            Ok(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    i as f64
                } else if let Ok(f) = s.parse::<f64>() {
                    f
                } else {
                    return Some(OwnedValue::Null);
                }
            }
            Err(_) => return Some(OwnedValue::Null),
        },
        _ => return Some(OwnedValue::Null),
    };

    let sign = if num > 0.0 {
        1
    } else if num < 0.0 {
        -1
    } else {
        0
    };

    Some(OwnedValue::Integer(sign))
}

/// Generates the Soundex code for a given word
pub fn exec_soundex(reg: &OwnedValue) -> OwnedValue {
    let s = match reg {
        OwnedValue::Null => return OwnedValue::build_text("?000"),
        OwnedValue::Text(s) => {
            // return ?000 if non ASCII alphabet character is found
            if !s.as_str().chars().all(|c| c.is_ascii_alphabetic()) {
                return OwnedValue::build_text("?000");
            }
            s.clone()
        }
        _ => return OwnedValue::build_text("?000"), // For unsupported types, return NULL
    };

    // Remove numbers and spaces
    let word: String = s
        .as_str()
        .chars()
        .filter(|c| !c.is_ascii_digit())
        .collect::<String>()
        .replace(" ", "");
    if word.is_empty() {
        return OwnedValue::build_text("0000");
    }

    let soundex_code = |c| match c {
        'b' | 'f' | 'p' | 'v' => Some('1'),
        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
        'd' | 't' => Some('3'),
        'l' => Some('4'),
        'm' | 'n' => Some('5'),
        'r' => Some('6'),
        _ => None,
    };

    // Convert the word to lowercase for consistent lookups
    let word = word.to_lowercase();
    let first_letter = word.chars().next().unwrap();

    // Remove all occurrences of 'h' and 'w' except the first letter
    let code: String = word
        .chars()
        .skip(1)
        .filter(|&ch| ch != 'h' && ch != 'w')
        .fold(first_letter.to_string(), |mut acc, ch| {
            acc.push(ch);
            acc
        });

    // Replace consonants with digits based on Soundex mapping
    let tmp: String = code
        .chars()
        .map(|ch| match soundex_code(ch) {
            Some(code) => code.to_string(),
            None => ch.to_string(),
        })
        .collect();

    // Remove adjacent same digits
    let tmp = tmp.chars().fold(String::new(), |mut acc, ch| {
        if !acc.ends_with(ch) {
            acc.push(ch);
        }
        acc
    });

    // Remove all occurrences of a, e, i, o, u, y except the first letter
    let mut result = tmp
        .chars()
        .enumerate()
        .filter(|(i, ch)| *i == 0 || !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'))
        .map(|(_, ch)| ch)
        .collect::<String>();

    // If the first symbol is a digit, replace it with the saved first letter
    if let Some(first_digit) = result.chars().next() {
        if first_digit.is_ascii_digit() {
            result.replace_range(0..1, &first_letter.to_string());
        }
    }

    // Append zeros if the result contains less than 4 characters
    while result.len() < 4 {
        result.push('0');
    }

    // Retain the first 4 characters and convert to uppercase
    result.truncate(4);
    OwnedValue::build_text(&result.to_uppercase())
}

fn exec_abs(reg: &OwnedValue) -> Result<OwnedValue> {
    match reg {
        OwnedValue::Integer(x) => {
            match i64::checked_abs(*x) {
                Some(y) => Ok(OwnedValue::Integer(y)),
                // Special case: if we do the abs of "-9223372036854775808", it causes overflow.
                // return IntegerOverflow error
                None => Err(LimboError::IntegerOverflow),
            }
        }
        OwnedValue::Float(x) => {
            if x < &0.0 {
                Ok(OwnedValue::Float(-x))
            } else {
                Ok(OwnedValue::Float(*x))
            }
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Ok(OwnedValue::Float(0.0)),
    }
}

fn exec_random() -> OwnedValue {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).unwrap();
    let random_number = i64::from_ne_bytes(buf);
    OwnedValue::Integer(random_number)
}

fn exec_randomblob(reg: &OwnedValue) -> OwnedValue {
    let length = match reg {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(t) => t.as_str().parse().unwrap_or(1),
        _ => 1,
    }
    .max(1) as usize;

    let mut blob: Vec<u8> = vec![0; length];
    getrandom::getrandom(&mut blob).expect("Failed to generate random blob");
    OwnedValue::Blob(blob)
}

fn exec_quote(value: &OwnedValue) -> OwnedValue {
    match value {
        OwnedValue::Null => OwnedValue::build_text("NULL"),
        OwnedValue::Integer(_) | OwnedValue::Float(_) => value.to_owned(),
        OwnedValue::Blob(_) => todo!(),
        OwnedValue::Text(s) => {
            let mut quoted = String::with_capacity(s.as_str().len() + 2);
            quoted.push('\'');
            for c in s.as_str().chars() {
                if c == '\0' {
                    break;
                } else if c == '\'' {
                    quoted.push('\'');
                    quoted.push(c);
                } else {
                    quoted.push(c);
                }
            }
            quoted.push('\'');
            OwnedValue::build_text(&quoted)
        }
    }
}

fn exec_char(values: &[Register]) -> OwnedValue {
    let result: String = values
        .iter()
        .filter_map(|x| {
            if let OwnedValue::Integer(i) = x.get_owned_value() {
                Some(*i as u8 as char)
            } else {
                None
            }
        })
        .collect();
    OwnedValue::build_text(&result)
}

fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

// Implements LIKE pattern matching. Caches the constructed regex if a cache is provided
fn exec_like(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_like_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_like_regex(pattern);
        re.is_match(text)
    }
}

fn exec_min(regs: &[Register]) -> OwnedValue {
    regs.iter()
        .map(|v| v.get_owned_value())
        .min()
        .map(|v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_max(regs: &[Register]) -> OwnedValue {
    regs.iter()
        .map(|v| v.get_owned_value())
        .max()
        .map(|v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_nullif(first_value: &OwnedValue, second_value: &OwnedValue) -> OwnedValue {
    if first_value != second_value {
        first_value.clone()
    } else {
        OwnedValue::Null
    }
}

fn exec_substring(
    str_value: &OwnedValue,
    start_value: &OwnedValue,
    length_value: Option<&OwnedValue>,
) -> OwnedValue {
    if let (OwnedValue::Text(str), OwnedValue::Integer(start)) = (str_value, start_value) {
        let str_len = str.as_str().len() as i64;

        // The left-most character of X is number 1.
        // If Y is negative then the first character of the substring is found by counting from the right rather than the left.
        let first_position = if *start < 0 {
            str_len.saturating_sub((*start).abs())
        } else {
            *start - 1
        };
        // If Z is negative then the abs(Z) characters preceding the Y-th character are returned.
        let last_position = match length_value {
            Some(OwnedValue::Integer(length)) => first_position + *length,
            _ => str_len,
        };
        let (start, end) = if first_position <= last_position {
            (first_position, last_position)
        } else {
            (last_position, first_position)
        };
        OwnedValue::build_text(
            &str.as_str()[start.clamp(-0, str_len) as usize..end.clamp(0, str_len) as usize],
        )
    } else {
        OwnedValue::Null
    }
}

fn exec_instr(reg: &OwnedValue, pattern: &OwnedValue) -> OwnedValue {
    if reg == &OwnedValue::Null || pattern == &OwnedValue::Null {
        return OwnedValue::Null;
    }

    if let (OwnedValue::Blob(reg), OwnedValue::Blob(pattern)) = (reg, pattern) {
        let result = reg
            .windows(pattern.len())
            .position(|window| window == *pattern)
            .map_or(0, |i| i + 1);
        return OwnedValue::Integer(result as i64);
    }

    let reg_str;
    let reg = match reg {
        OwnedValue::Text(s) => s.as_str(),
        _ => {
            reg_str = reg.to_string();
            reg_str.as_str()
        }
    };

    let pattern_str;
    let pattern = match pattern {
        OwnedValue::Text(s) => s.as_str(),
        _ => {
            pattern_str = pattern.to_string();
            pattern_str.as_str()
        }
    };

    match reg.find(pattern) {
        Some(position) => OwnedValue::Integer(position as i64 + 1),
        None => OwnedValue::Integer(0),
    }
}

fn exec_typeof(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::build_text("null"),
        OwnedValue::Integer(_) => OwnedValue::build_text("integer"),
        OwnedValue::Float(_) => OwnedValue::build_text("real"),
        OwnedValue::Text(_) => OwnedValue::build_text("text"),
        OwnedValue::Blob(_) => OwnedValue::build_text("blob"),
    }
}

fn exec_hex(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            OwnedValue::build_text(&hex::encode_upper(text))
        }
        _ => OwnedValue::Null,
    }
}

fn exec_unhex(reg: &OwnedValue, ignored_chars: Option<&OwnedValue>) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::Null,
        _ => match ignored_chars {
            None => match hex::decode(reg.to_string()) {
                Ok(bytes) => OwnedValue::Blob(bytes),
                Err(_) => OwnedValue::Null,
            },
            Some(ignore) => match ignore {
                OwnedValue::Text(_) => {
                    let pat = ignore.to_string();
                    let trimmed = reg
                        .to_string()
                        .trim_start_matches(|x| pat.contains(x))
                        .trim_end_matches(|x| pat.contains(x))
                        .to_string();
                    match hex::decode(trimmed) {
                        Ok(bytes) => OwnedValue::Blob(bytes),
                        Err(_) => OwnedValue::Null,
                    }
                }
                _ => OwnedValue::Null,
            },
        },
    }
}

fn exec_unicode(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            if let Some(first_char) = text.chars().next() {
                OwnedValue::Integer(first_char as u32 as i64)
            } else {
                OwnedValue::Null
            }
        }
        _ => OwnedValue::Null,
    }
}

fn _to_float(reg: &OwnedValue) -> f64 {
    match reg {
        OwnedValue::Text(x) => match cast_text_to_numeric(x.as_str()) {
            OwnedValue::Integer(i) => i as f64,
            OwnedValue::Float(f) => f,
            _ => unreachable!(),
        },
        OwnedValue::Integer(x) => *x as f64,
        OwnedValue::Float(x) => *x,
        _ => 0.0,
    }
}

fn exec_round(reg: &OwnedValue, precision: Option<&OwnedValue>) -> OwnedValue {
    let reg = _to_float(reg);
    let round = |reg: f64, f: f64| {
        let precision = if f < 1.0 { 0.0 } else { f };
        OwnedValue::Float(reg.round_to_precision(precision as i32))
    };
    match precision {
        Some(OwnedValue::Text(x)) => match cast_text_to_numeric(x.as_str()) {
            OwnedValue::Integer(i) => round(reg, i as f64),
            OwnedValue::Float(f) => round(reg, f),
            _ => unreachable!(),
        },
        Some(OwnedValue::Integer(i)) => round(reg, *i as f64),
        Some(OwnedValue::Float(f)) => round(reg, *f),
        None => round(reg, 0.0),
        _ => OwnedValue::Null,
    }
}

// Implements TRIM pattern matching.
fn exec_trim(reg: &OwnedValue, pattern: Option<&OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(reg.to_string().trim_matches(&pattern_chars[..]))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::build_text(t.as_str().trim()),
        (reg, _) => reg.to_owned(),
    }
}

// Implements LTRIM pattern matching.
fn exec_ltrim(reg: &OwnedValue, pattern: Option<&OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(reg.to_string().trim_start_matches(&pattern_chars[..]))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::build_text(t.as_str().trim_start()),
        (reg, _) => reg.to_owned(),
    }
}

// Implements RTRIM pattern matching.
fn exec_rtrim(reg: &OwnedValue, pattern: Option<&OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(reg.to_string().trim_end_matches(&pattern_chars[..]))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::build_text(t.as_str().trim_end()),
        (reg, _) => reg.to_owned(),
    }
}

fn exec_zeroblob(req: &OwnedValue) -> OwnedValue {
    let length: i64 = match req {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(s) => s.as_str().parse().unwrap_or(0),
        _ => 0,
    };
    OwnedValue::Blob(vec![0; length.max(0) as usize])
}

// exec_if returns whether you should jump
fn exec_if(reg: &OwnedValue, jump_if_null: bool, not: bool) -> bool {
    match reg {
        OwnedValue::Integer(0) | OwnedValue::Float(0.0) => not,
        OwnedValue::Integer(_) | OwnedValue::Float(_) => !not,
        OwnedValue::Null => jump_if_null,
        _ => false,
    }
}

fn apply_affinity_char(target: &mut Register, affinity: Affinity) -> bool {
    if let Register::OwnedValue(value) = target {
        if matches!(value, OwnedValue::Blob(_)) {
            return true;
        }
        match affinity {
            Affinity::Blob => return true,
            Affinity::Text => {
                if matches!(value, OwnedValue::Text(_) | OwnedValue::Null) {
                    return true;
                }
                let text = value.to_string();
                *value = OwnedValue::Text(text.into());
                return true;
            }
            Affinity::Integer | Affinity::Numeric => {
                if matches!(value, OwnedValue::Integer(_)) {
                    return true;
                }
                if !matches!(value, OwnedValue::Text(_) | OwnedValue::Float(_)) {
                    return true;
                }

                if let OwnedValue::Float(fl) = *value {
                    if let Ok(int) = cast_real_to_integer(fl).map(OwnedValue::Integer) {
                        *value = int;
                        return true;
                    }
                    return false;
                }

                let text = value.to_text().unwrap();
                let Ok(num) = checked_cast_text_to_numeric(&text) else {
                    return false;
                };

                *value = match &num {
                    OwnedValue::Float(fl) => {
                        cast_real_to_integer(*fl)
                            .map(OwnedValue::Integer)
                            .unwrap_or(num);
                        return true;
                    }
                    OwnedValue::Integer(_) if text.starts_with("0x") => {
                        return false;
                    }
                    _ => num,
                };
            }

            Affinity::Real => {
                if let OwnedValue::Integer(i) = value {
                    *value = OwnedValue::Float(*i as f64);
                    return true;
                } else if let OwnedValue::Text(t) = value {
                    if t.as_str().starts_with("0x") {
                        return false;
                    }
                    if let Ok(num) = checked_cast_text_to_numeric(t.as_str()) {
                        *value = num;
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        };
    }
    return true;
}

fn exec_cast(value: &OwnedValue, datatype: &str) -> OwnedValue {
    if matches!(value, OwnedValue::Null) {
        return OwnedValue::Null;
    }
    match affinity(datatype) {
        // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
        // Historically called NONE, but it's the same as BLOB
        Affinity::Blob => {
            // Convert to TEXT first, then interpret as BLOB
            // TODO: handle encoding
            let text = value.to_string();
            OwnedValue::Blob(text.into_bytes())
        }
        // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
        // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
        Affinity::Text => {
            // Convert everything to text representation
            // TODO: handle encoding and whatever sqlite3_snprintf does
            OwnedValue::build_text(&value.to_string())
        }
        Affinity::Real => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_real(&text)
            }
            OwnedValue::Text(t) => cast_text_to_real(t.as_str()),
            OwnedValue::Integer(i) => OwnedValue::Float(*i as f64),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => OwnedValue::Float(0.0),
        },
        Affinity::Integer => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_integer(&text)
            }
            OwnedValue::Text(t) => cast_text_to_integer(t.as_str()),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            // A cast of a REAL value into an INTEGER results in the integer between the REAL value and zero
            // that is closest to the REAL value. If a REAL is greater than the greatest possible signed integer (+9223372036854775807)
            // then the result is the greatest possible signed integer and if the REAL is less than the least possible signed integer (-9223372036854775808)
            // then the result is the least possible signed integer.
            OwnedValue::Float(f) => {
                let i = f.trunc() as i128;
                if i > i64::MAX as i128 {
                    OwnedValue::Integer(i64::MAX)
                } else if i < i64::MIN as i128 {
                    OwnedValue::Integer(i64::MIN)
                } else {
                    OwnedValue::Integer(i as i64)
                }
            }
            _ => OwnedValue::Integer(0),
        },
        Affinity::Numeric => match value {
            OwnedValue::Blob(b) => {
                let text = String::from_utf8_lossy(b);
                cast_text_to_numeric(&text)
            }
            OwnedValue::Text(t) => cast_text_to_numeric(t.as_str()),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => value.clone(), // TODO probably wrong
        },
    }
}

fn exec_replace(source: &OwnedValue, pattern: &OwnedValue, replacement: &OwnedValue) -> OwnedValue {
    // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
    // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
    // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

    // If any of the arguments is NULL, the result is NULL.
    if matches!(source, OwnedValue::Null)
        || matches!(pattern, OwnedValue::Null)
        || matches!(replacement, OwnedValue::Null)
    {
        return OwnedValue::Null;
    }

    let source = exec_cast(source, "TEXT");
    let pattern = exec_cast(pattern, "TEXT");
    let replacement = exec_cast(replacement, "TEXT");

    // If any of the casts failed, panic as text casting is not expected to fail.
    match (&source, &pattern, &replacement) {
        (OwnedValue::Text(source), OwnedValue::Text(pattern), OwnedValue::Text(replacement)) => {
            if pattern.as_str().is_empty() {
                return OwnedValue::Text(source.clone());
            }

            let result = source
                .as_str()
                .replace(pattern.as_str(), replacement.as_str());
            OwnedValue::build_text(&result)
        }
        _ => unreachable!("text cast should never fail"),
    }
}

fn execute_sqlite_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{}.{}.{}", major, minor, release)
}

fn to_f64(reg: &OwnedValue) -> Option<f64> {
    match reg {
        OwnedValue::Integer(i) => Some(*i as f64),
        OwnedValue::Float(f) => Some(*f),
        OwnedValue::Text(t) => t.as_str().parse::<f64>().ok(),
        _ => None,
    }
}

fn exec_math_unary(reg: &OwnedValue, function: &MathFunc) -> OwnedValue {
    // In case of some functions and integer input, return the input as is
    if let OwnedValue::Integer(_) = reg {
        if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
        {
            return reg.clone();
        }
    }

    let f = match to_f64(reg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Acos => libm::acos(f),
        MathFunc::Acosh => libm::acosh(f),
        MathFunc::Asin => libm::asin(f),
        MathFunc::Asinh => libm::asinh(f),
        MathFunc::Atan => libm::atan(f),
        MathFunc::Atanh => libm::atanh(f),
        MathFunc::Ceil | MathFunc::Ceiling => libm::ceil(f),
        MathFunc::Cos => libm::cos(f),
        MathFunc::Cosh => libm::cosh(f),
        MathFunc::Degrees => f.to_degrees(),
        MathFunc::Exp => libm::exp(f),
        MathFunc::Floor => libm::floor(f),
        MathFunc::Ln => libm::log(f),
        MathFunc::Log10 => libm::log10(f),
        MathFunc::Log2 => libm::log2(f),
        MathFunc::Radians => f.to_radians(),
        MathFunc::Sin => libm::sin(f),
        MathFunc::Sinh => libm::sinh(f),
        MathFunc::Sqrt => libm::sqrt(f),
        MathFunc::Tan => libm::tan(f),
        MathFunc::Tanh => libm::tanh(f),
        MathFunc::Trunc => libm::trunc(f),
        _ => unreachable!("Unexpected mathematical unary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_binary(lhs: &OwnedValue, rhs: &OwnedValue, function: &MathFunc) -> OwnedValue {
    let lhs = match to_f64(lhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let rhs = match to_f64(rhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Atan2 => libm::atan2(lhs, rhs),
        MathFunc::Mod => libm::fmod(lhs, rhs),
        MathFunc::Pow | MathFunc::Power => libm::pow(lhs, rhs),
        _ => unreachable!("Unexpected mathematical binary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_log(arg: &OwnedValue, base: Option<&OwnedValue>) -> OwnedValue {
    let f = match to_f64(arg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let base = match base {
        Some(base) => match to_f64(base) {
            Some(f) => f,
            None => return OwnedValue::Null,
        },
        None => 10.0,
    };

    if f <= 0.0 || base <= 0.0 || base == 1.0 {
        return OwnedValue::Null;
    }
    let log_x = libm::log(f);
    let log_base = libm::log(base);
    let result = log_x / log_base;
    OwnedValue::Float(result)
}

fn exec_likely(reg: &OwnedValue) -> OwnedValue {
    reg.clone()
}

fn exec_likelihood(reg: &OwnedValue, _probability: &OwnedValue) -> OwnedValue {
    reg.clone()
}

pub fn exec_add(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (Numeric::from(lhs) + Numeric::from(rhs)).into()
}

pub fn exec_subtract(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (Numeric::from(lhs) - Numeric::from(rhs)).into()
}

pub fn exec_multiply(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (Numeric::from(lhs) * Numeric::from(rhs)).into()
}

pub fn exec_divide(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (Numeric::from(lhs) / Numeric::from(rhs)).into()
}

pub fn exec_bit_and(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (NullableInteger::from(lhs) & NullableInteger::from(rhs)).into()
}

pub fn exec_bit_or(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (NullableInteger::from(lhs) | NullableInteger::from(rhs)).into()
}

pub fn exec_remainder(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    let convert_to_float = matches!(Numeric::from(lhs), Numeric::Float(_))
        || matches!(Numeric::from(rhs), Numeric::Float(_));

    match NullableInteger::from(lhs) % NullableInteger::from(rhs) {
        NullableInteger::Null => OwnedValue::Null,
        NullableInteger::Integer(v) => {
            if convert_to_float {
                OwnedValue::Float(v as f64)
            } else {
                OwnedValue::Integer(v)
            }
        }
    }
}

pub fn exec_bit_not(reg: &OwnedValue) -> OwnedValue {
    (!NullableInteger::from(reg)).into()
}

pub fn exec_shift_left(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (NullableInteger::from(lhs) << NullableInteger::from(rhs)).into()
}

pub fn exec_shift_right(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    (NullableInteger::from(lhs) >> NullableInteger::from(rhs)).into()
}

pub fn exec_boolean_not(reg: &OwnedValue) -> OwnedValue {
    match Numeric::from(reg).try_into_bool() {
        None => OwnedValue::Null,
        Some(v) => OwnedValue::Integer(!v as i64),
    }
}
pub fn exec_concat(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    match (lhs, rhs) {
        (OwnedValue::Text(lhs_text), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(&(lhs_text.as_str().to_string() + rhs_text.as_str()))
        }
        (OwnedValue::Text(lhs_text), OwnedValue::Integer(rhs_int)) => {
            OwnedValue::build_text(&(lhs_text.as_str().to_string() + &rhs_int.to_string()))
        }
        (OwnedValue::Text(lhs_text), OwnedValue::Float(rhs_float)) => {
            OwnedValue::build_text(&(lhs_text.as_str().to_string() + &rhs_float.to_string()))
        }
        (OwnedValue::Integer(lhs_int), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(&(lhs_int.to_string() + rhs_text.as_str()))
        }
        (OwnedValue::Integer(lhs_int), OwnedValue::Integer(rhs_int)) => {
            OwnedValue::build_text(&(lhs_int.to_string() + &rhs_int.to_string()))
        }
        (OwnedValue::Integer(lhs_int), OwnedValue::Float(rhs_float)) => {
            OwnedValue::build_text(&(lhs_int.to_string() + &rhs_float.to_string()))
        }
        (OwnedValue::Float(lhs_float), OwnedValue::Text(rhs_text)) => {
            OwnedValue::build_text(&(lhs_float.to_string() + rhs_text.as_str()))
        }
        (OwnedValue::Float(lhs_float), OwnedValue::Integer(rhs_int)) => {
            OwnedValue::build_text(&(lhs_float.to_string() + &rhs_int.to_string()))
        }
        (OwnedValue::Float(lhs_float), OwnedValue::Float(rhs_float)) => {
            OwnedValue::build_text(&(lhs_float.to_string() + &rhs_float.to_string()))
        }
        (OwnedValue::Null, _) | (_, OwnedValue::Null) => OwnedValue::Null,
        (OwnedValue::Blob(_), _) | (_, OwnedValue::Blob(_)) => {
            todo!("TODO: Handle Blob conversion to String")
        }
    }
}

pub fn exec_and(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    match (
        Numeric::from(lhs).try_into_bool(),
        Numeric::from(rhs).try_into_bool(),
    ) {
        (Some(false), _) | (_, Some(false)) => OwnedValue::Integer(0),
        (None, _) | (_, None) => OwnedValue::Null,
        _ => OwnedValue::Integer(1),
    }
}

pub fn exec_or(lhs: &OwnedValue, rhs: &OwnedValue) -> OwnedValue {
    match (
        Numeric::from(lhs).try_into_bool(),
        Numeric::from(rhs).try_into_bool(),
    ) {
        (Some(true), _) | (_, Some(true)) => OwnedValue::Integer(1),
        (None, _) | (_, None) => OwnedValue::Null,
        _ => OwnedValue::Integer(0),
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{OwnedValue, Text};

    use super::{exec_add, exec_or};

    #[test]
    fn test_exec_add() {
        let inputs = vec![
            (OwnedValue::Integer(3), OwnedValue::Integer(1)),
            (OwnedValue::Float(3.0), OwnedValue::Float(1.0)),
            (OwnedValue::Float(3.0), OwnedValue::Integer(1)),
            (OwnedValue::Integer(3), OwnedValue::Float(1.0)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Float(1.0)),
            (OwnedValue::Null, OwnedValue::Text(Text::from_str("2"))),
            (OwnedValue::Integer(1), OwnedValue::Null),
            (OwnedValue::Float(1.0), OwnedValue::Null),
            (OwnedValue::Text(Text::from_str("1")), OwnedValue::Null),
            (
                OwnedValue::Text(Text::from_str("1")),
                OwnedValue::Text(Text::from_str("3")),
            ),
            (
                OwnedValue::Text(Text::from_str("1.0")),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Text(Text::from_str("1.0")),
                OwnedValue::Float(3.0),
            ),
            (
                OwnedValue::Text(Text::from_str("1.0")),
                OwnedValue::Integer(3),
            ),
            (
                OwnedValue::Float(1.0),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Integer(1),
                OwnedValue::Text(Text::from_str("3")),
            ),
        ];

        let outputs = [
            OwnedValue::Integer(4),
            OwnedValue::Float(4.0),
            OwnedValue::Float(4.0),
            OwnedValue::Float(4.0),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(4),
            OwnedValue::Float(4.0),
            OwnedValue::Float(4.0),
            OwnedValue::Float(4.0),
            OwnedValue::Float(4.0),
            OwnedValue::Float(4.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_add(lhs, rhs),
                outputs[i],
                "Wrong ADD for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }

    use super::exec_subtract;

    #[test]
    fn test_exec_subtract() {
        let inputs = vec![
            (OwnedValue::Integer(3), OwnedValue::Integer(1)),
            (OwnedValue::Float(3.0), OwnedValue::Float(1.0)),
            (OwnedValue::Float(3.0), OwnedValue::Integer(1)),
            (OwnedValue::Integer(3), OwnedValue::Float(1.0)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Float(1.0)),
            (OwnedValue::Null, OwnedValue::Text(Text::from_str("1"))),
            (OwnedValue::Integer(1), OwnedValue::Null),
            (OwnedValue::Float(1.0), OwnedValue::Null),
            (OwnedValue::Text(Text::from_str("4")), OwnedValue::Null),
            (
                OwnedValue::Text(Text::from_str("1")),
                OwnedValue::Text(Text::from_str("3")),
            ),
            (
                OwnedValue::Text(Text::from_str("1.0")),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Text(Text::from_str("1.0")),
                OwnedValue::Float(3.0),
            ),
            (
                OwnedValue::Text(Text::from_str("1.0")),
                OwnedValue::Integer(3),
            ),
            (
                OwnedValue::Float(1.0),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Integer(1),
                OwnedValue::Text(Text::from_str("3")),
            ),
        ];

        let outputs = [
            OwnedValue::Integer(2),
            OwnedValue::Float(2.0),
            OwnedValue::Float(2.0),
            OwnedValue::Float(2.0),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(-2),
            OwnedValue::Float(-2.0),
            OwnedValue::Float(-2.0),
            OwnedValue::Float(-2.0),
            OwnedValue::Float(-2.0),
            OwnedValue::Float(-2.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_subtract(lhs, rhs),
                outputs[i],
                "Wrong subtract for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }
    use super::exec_multiply;

    #[test]
    fn test_exec_multiply() {
        let inputs = vec![
            (OwnedValue::Integer(3), OwnedValue::Integer(2)),
            (OwnedValue::Float(3.0), OwnedValue::Float(2.0)),
            (OwnedValue::Float(3.0), OwnedValue::Integer(2)),
            (OwnedValue::Integer(3), OwnedValue::Float(2.0)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Float(1.0)),
            (OwnedValue::Null, OwnedValue::Text(Text::from_str("1"))),
            (OwnedValue::Integer(1), OwnedValue::Null),
            (OwnedValue::Float(1.0), OwnedValue::Null),
            (OwnedValue::Text(Text::from_str("4")), OwnedValue::Null),
            (
                OwnedValue::Text(Text::from_str("2")),
                OwnedValue::Text(Text::from_str("3")),
            ),
            (
                OwnedValue::Text(Text::from_str("2.0")),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Text(Text::from_str("2.0")),
                OwnedValue::Float(3.0),
            ),
            (
                OwnedValue::Text(Text::from_str("2.0")),
                OwnedValue::Integer(3),
            ),
            (
                OwnedValue::Float(2.0),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Integer(2),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
        ];

        let outputs = [
            OwnedValue::Integer(6),
            OwnedValue::Float(6.0),
            OwnedValue::Float(6.0),
            OwnedValue::Float(6.0),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(6),
            OwnedValue::Float(6.0),
            OwnedValue::Float(6.0),
            OwnedValue::Float(6.0),
            OwnedValue::Float(6.0),
            OwnedValue::Float(6.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_multiply(lhs, rhs),
                outputs[i],
                "Wrong multiply for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }
    use super::exec_divide;

    #[test]
    fn test_exec_divide() {
        let inputs = vec![
            (OwnedValue::Integer(1), OwnedValue::Integer(0)),
            (OwnedValue::Float(1.0), OwnedValue::Float(0.0)),
            (OwnedValue::Integer(i64::MIN), OwnedValue::Integer(-1)),
            (OwnedValue::Float(6.0), OwnedValue::Float(2.0)),
            (OwnedValue::Float(6.0), OwnedValue::Integer(2)),
            (OwnedValue::Integer(6), OwnedValue::Integer(2)),
            (OwnedValue::Null, OwnedValue::Integer(2)),
            (OwnedValue::Integer(2), OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Null),
            (
                OwnedValue::Text(Text::from_str("6")),
                OwnedValue::Text(Text::from_str("2")),
            ),
            (
                OwnedValue::Text(Text::from_str("6")),
                OwnedValue::Integer(2),
            ),
        ];

        let outputs = [
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Float(9.223372036854776e18),
            OwnedValue::Float(3.0),
            OwnedValue::Float(3.0),
            OwnedValue::Float(3.0),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Float(3.0),
            OwnedValue::Float(3.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_divide(lhs, rhs),
                outputs[i],
                "Wrong divide for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }

    use super::exec_remainder;
    #[test]
    fn test_exec_remainder() {
        let inputs = vec![
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Float(1.0)),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Text(Text::from_str("1"))),
            (OwnedValue::Float(1.0), OwnedValue::Null),
            (OwnedValue::Integer(1), OwnedValue::Null),
            (OwnedValue::Integer(12), OwnedValue::Integer(0)),
            (OwnedValue::Float(12.0), OwnedValue::Float(0.0)),
            (OwnedValue::Float(12.0), OwnedValue::Integer(0)),
            (OwnedValue::Integer(12), OwnedValue::Float(0.0)),
            (OwnedValue::Integer(i64::MIN), OwnedValue::Integer(-1)),
            (OwnedValue::Integer(12), OwnedValue::Integer(3)),
            (OwnedValue::Float(12.0), OwnedValue::Float(3.0)),
            (OwnedValue::Float(12.0), OwnedValue::Integer(3)),
            (OwnedValue::Integer(12), OwnedValue::Float(3.0)),
            (OwnedValue::Integer(12), OwnedValue::Integer(-3)),
            (OwnedValue::Float(12.0), OwnedValue::Float(-3.0)),
            (OwnedValue::Float(12.0), OwnedValue::Integer(-3)),
            (OwnedValue::Integer(12), OwnedValue::Float(-3.0)),
            (
                OwnedValue::Text(Text::from_str("12.0")),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
            (
                OwnedValue::Text(Text::from_str("12.0")),
                OwnedValue::Float(3.0),
            ),
            (
                OwnedValue::Float(12.0),
                OwnedValue::Text(Text::from_str("3.0")),
            ),
        ];
        let outputs = vec![
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Float(0.0),
            OwnedValue::Integer(0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
            OwnedValue::Integer(0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
            OwnedValue::Float(0.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );

        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_remainder(lhs, rhs),
                outputs[i],
                "Wrong remainder for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }

    use super::exec_and;

    #[test]
    fn test_exec_and() {
        let inputs = vec![
            (OwnedValue::Integer(0), OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Float(0.0), OwnedValue::Null),
            (OwnedValue::Integer(1), OwnedValue::Float(2.2)),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(Text::from_str("string")),
            ),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(Text::from_str("1")),
            ),
            (
                OwnedValue::Integer(1),
                OwnedValue::Text(Text::from_str("1")),
            ),
        ];
        let outputs = [
            OwnedValue::Integer(0),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(0),
            OwnedValue::Integer(1),
            OwnedValue::Integer(0),
            OwnedValue::Integer(0),
            OwnedValue::Integer(1),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_and(lhs, rhs),
                outputs[i],
                "Wrong AND for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }

    #[test]
    fn test_exec_or() {
        let inputs = vec![
            (OwnedValue::Integer(0), OwnedValue::Null),
            (OwnedValue::Null, OwnedValue::Integer(1)),
            (OwnedValue::Null, OwnedValue::Null),
            (OwnedValue::Float(0.0), OwnedValue::Null),
            (OwnedValue::Integer(1), OwnedValue::Float(2.2)),
            (OwnedValue::Float(0.0), OwnedValue::Integer(0)),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(Text::from_str("string")),
            ),
            (
                OwnedValue::Integer(0),
                OwnedValue::Text(Text::from_str("1")),
            ),
            (OwnedValue::Integer(0), OwnedValue::Text(Text::from_str(""))),
        ];
        let outputs = [
            OwnedValue::Null,
            OwnedValue::Integer(1),
            OwnedValue::Null,
            OwnedValue::Null,
            OwnedValue::Integer(1),
            OwnedValue::Integer(0),
            OwnedValue::Integer(0),
            OwnedValue::Integer(1),
            OwnedValue::Integer(0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                exec_or(lhs, rhs),
                outputs[i],
                "Wrong OR for lhs: {}, rhs: {}",
                lhs,
                rhs
            );
        }
    }

    use crate::vdbe::{
        execute::{exec_likelihood, exec_likely, exec_replace},
        Bitfield, Register,
    };

    use super::{
        exec_abs, exec_char, exec_hex, exec_if, exec_instr, exec_length, exec_like, exec_lower,
        exec_ltrim, exec_max, exec_min, exec_nullif, exec_quote, exec_random, exec_randomblob,
        exec_round, exec_rtrim, exec_sign, exec_soundex, exec_substring, exec_trim, exec_typeof,
        exec_unhex, exec_unicode, exec_upper, exec_zeroblob, execute_sqlite_version,
    };
    use std::collections::HashMap;

    #[test]
    fn test_length() {
        let input_str = OwnedValue::build_text("bob");
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_str), expected_len);

        let input_integer = OwnedValue::Integer(123);
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_integer), expected_len);

        let input_float = OwnedValue::Float(123.456);
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&input_float), expected_len);

        let expected_blob = OwnedValue::Blob("example".as_bytes().to_vec());
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&expected_blob), expected_len);
    }

    #[test]
    fn test_quote() {
        let input = OwnedValue::build_text("abc\0edf");
        let expected = OwnedValue::build_text("'abc'");
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected = OwnedValue::Integer(123);
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::build_text("hello''world");
        let expected = OwnedValue::build_text("'hello''''world'");
        assert_eq!(exec_quote(&input), expected);
    }

    #[test]
    fn test_typeof() {
        let input = OwnedValue::Null;
        let expected: OwnedValue = OwnedValue::build_text("null");
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected: OwnedValue = OwnedValue::build_text("integer");
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Float(123.456);
        let expected: OwnedValue = OwnedValue::build_text("real");
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::build_text("hello");
        let expected: OwnedValue = OwnedValue::build_text("text");
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Blob("limbo".as_bytes().to_vec());
        let expected: OwnedValue = OwnedValue::build_text("blob");
        assert_eq!(exec_typeof(&input), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            exec_unicode(&OwnedValue::build_text("a")),
            OwnedValue::Integer(97)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::build_text("😊")),
            OwnedValue::Integer(128522)
        );
        assert_eq!(exec_unicode(&OwnedValue::build_text("")), OwnedValue::Null);
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(23)),
            OwnedValue::Integer(50)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(0.0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(23.45)),
            OwnedValue::Integer(50)
        );
        assert_eq!(exec_unicode(&OwnedValue::Null), OwnedValue::Null);
        assert_eq!(
            exec_unicode(&OwnedValue::Blob("example".as_bytes().to_vec())),
            OwnedValue::Integer(101)
        );
    }

    #[test]
    fn test_min_max() {
        let input_int_vec = vec![
            Register::OwnedValue(OwnedValue::Integer(-1)),
            Register::OwnedValue(OwnedValue::Integer(10)),
        ];
        assert_eq!(exec_min(&input_int_vec), OwnedValue::Integer(-1));
        assert_eq!(exec_max(&input_int_vec), OwnedValue::Integer(10));

        let str1 = Register::OwnedValue(OwnedValue::build_text("A"));
        let str2 = Register::OwnedValue(OwnedValue::build_text("z"));
        let input_str_vec = vec![str2, str1.clone()];
        assert_eq!(exec_min(&input_str_vec), OwnedValue::build_text("A"));
        assert_eq!(exec_max(&input_str_vec), OwnedValue::build_text("z"));

        let input_null_vec = vec![
            Register::OwnedValue(OwnedValue::Null),
            Register::OwnedValue(OwnedValue::Null),
        ];
        assert_eq!(exec_min(&input_null_vec), OwnedValue::Null);
        assert_eq!(exec_max(&input_null_vec), OwnedValue::Null);

        let input_mixed_vec = vec![Register::OwnedValue(OwnedValue::Integer(10)), str1];
        assert_eq!(exec_min(&input_mixed_vec), OwnedValue::Integer(10));
        assert_eq!(exec_max(&input_mixed_vec), OwnedValue::build_text("A"));
    }

    #[test]
    fn test_trim() {
        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let expected_str = OwnedValue::build_text("Bob and Alice");
        assert_eq!(exec_trim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let pattern_str = OwnedValue::build_text("Bob and");
        let expected_str = OwnedValue::build_text("Alice");
        assert_eq!(exec_trim(&input_str, Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let expected_str = OwnedValue::build_text("Bob and Alice     ");
        assert_eq!(exec_ltrim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let pattern_str = OwnedValue::build_text("Bob and");
        let expected_str = OwnedValue::build_text("Alice     ");
        assert_eq!(exec_ltrim(&input_str, Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let expected_str = OwnedValue::build_text("     Bob and Alice");
        assert_eq!(exec_rtrim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let pattern_str = OwnedValue::build_text("Bob and");
        let expected_str = OwnedValue::build_text("     Bob and Alice");
        assert_eq!(exec_rtrim(&input_str, Some(&pattern_str)), expected_str);

        let input_str = OwnedValue::build_text("     Bob and Alice     ");
        let pattern_str = OwnedValue::build_text("and Alice");
        let expected_str = OwnedValue::build_text("     Bob");
        assert_eq!(exec_rtrim(&input_str, Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_soundex() {
        let input_str = OwnedValue::build_text("Pfister");
        let expected_str = OwnedValue::build_text("P236");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("husobee");
        let expected_str = OwnedValue::build_text("H210");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Tymczak");
        let expected_str = OwnedValue::build_text("T522");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Ashcraft");
        let expected_str = OwnedValue::build_text("A261");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Robert");
        let expected_str = OwnedValue::build_text("R163");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Rupert");
        let expected_str = OwnedValue::build_text("R163");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Rubin");
        let expected_str = OwnedValue::build_text("R150");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Kant");
        let expected_str = OwnedValue::build_text("K530");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("Knuth");
        let expected_str = OwnedValue::build_text("K530");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("x");
        let expected_str = OwnedValue::build_text("X000");
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text("闪电五连鞭");
        let expected_str = OwnedValue::build_text("?000");
        assert_eq!(exec_soundex(&input_str), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = OwnedValue::build_text("Limbo");
        let expected_str = OwnedValue::build_text("LIMBO");
        assert_eq!(exec_upper(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_upper(&input_int).unwrap(), input_int);
        assert_eq!(exec_upper(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_lower_case() {
        let input_str = OwnedValue::build_text("Limbo");
        let expected_str = OwnedValue::build_text("limbo");
        assert_eq!(exec_lower(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_lower(&input_int).unwrap(), input_int);
        assert_eq!(exec_lower(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_hex() {
        let input_str = OwnedValue::build_text("limbo");
        let expected_val = OwnedValue::build_text("6C696D626F");
        assert_eq!(exec_hex(&input_str), expected_val);

        let input_int = OwnedValue::Integer(100);
        let expected_val = OwnedValue::build_text("313030");
        assert_eq!(exec_hex(&input_int), expected_val);

        let input_float = OwnedValue::Float(12.34);
        let expected_val = OwnedValue::build_text("31322E3334");
        assert_eq!(exec_hex(&input_float), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = OwnedValue::build_text("6f");
        let expected = OwnedValue::Blob(vec![0x6f]);
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text("6f");
        let expected = OwnedValue::Blob(vec![0x6f]);
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text("611");
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text("");
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text("61x");
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);
    }

    #[test]
    fn test_abs() {
        let int_positive_reg = OwnedValue::Integer(10);
        let int_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&int_positive_reg).unwrap(), int_positive_reg);
        assert_eq!(exec_abs(&int_negative_reg).unwrap(), int_positive_reg);

        let float_positive_reg = OwnedValue::Integer(10);
        let float_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&float_positive_reg).unwrap(), float_positive_reg);
        assert_eq!(exec_abs(&float_negative_reg).unwrap(), float_positive_reg);

        assert_eq!(
            exec_abs(&OwnedValue::build_text("a")).unwrap(),
            OwnedValue::Float(0.0)
        );
        assert_eq!(exec_abs(&OwnedValue::Null).unwrap(), OwnedValue::Null);

        // ABS(i64::MIN) should return RuntimeError
        assert!(exec_abs(&OwnedValue::Integer(i64::MIN)).is_err());
    }

    #[test]
    fn test_char() {
        assert_eq!(
            exec_char(&[
                Register::OwnedValue(OwnedValue::Integer(108)),
                Register::OwnedValue(OwnedValue::Integer(105))
            ]),
            OwnedValue::build_text("li")
        );
        assert_eq!(exec_char(&[]), OwnedValue::build_text(""));
        assert_eq!(
            exec_char(&[Register::OwnedValue(OwnedValue::Null)]),
            OwnedValue::build_text("")
        );
        assert_eq!(
            exec_char(&[Register::OwnedValue(OwnedValue::build_text("a"))]),
            OwnedValue::build_text("")
        );
    }

    #[test]
    fn test_like_with_escape_or_regexmeta_chars() {
        assert!(exec_like(None, r#"\%A"#, r#"\A"#));
        assert!(exec_like(None, "%a%a", "aaaa"));
    }

    #[test]
    fn test_like_no_cache() {
        assert!(exec_like(None, "a%", "aaaa"));
        assert!(exec_like(None, "%a%a", "aaaa"));
        assert!(!exec_like(None, "%a.a", "aaaa"));
        assert!(!exec_like(None, "a.a%", "aaaa"));
        assert!(!exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));
    }

    #[test]
    fn test_random() {
        match exec_random() {
            OwnedValue::Integer(value) => {
                // Check that the value is within the range of i64
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "Random number out of range"
                );
            }
            _ => panic!("exec_random did not return an Integer variant"),
        }
    }

    #[test]
    fn test_exec_randomblob() {
        struct TestCase {
            input: OwnedValue,
            expected_len: usize,
        }

        let test_cases = vec![
            TestCase {
                input: OwnedValue::Integer(5),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::Integer(0),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Integer(-1),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text(""),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text("5"),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::build_text("0"),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text("-1"),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: OwnedValue::Float(-3.15),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Null,
                expected_len: 1,
            },
        ];

        for test_case in &test_cases {
            let result = exec_randomblob(&test_case.input);
            match result {
                OwnedValue::Blob(blob) => {
                    assert_eq!(blob.len(), test_case.expected_len);
                }
                _ => panic!("exec_randomblob did not return a Blob variant"),
            }
        }
    }

    #[test]
    fn test_exec_round() {
        let input_val = OwnedValue::Float(123.456);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(&precision_val)), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::build_text("1");
        let expected_val = OwnedValue::Float(123.5);
        assert_eq!(exec_round(&input_val, Some(&precision_val)), expected_val);

        let input_val = OwnedValue::build_text("123.456");
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(&precision_val)), expected_val);

        let input_val = OwnedValue::Integer(123);
        let precision_val = OwnedValue::Integer(1);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, Some(&precision_val)), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Float(100.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Null;
        assert_eq!(
            exec_round(&input_val, Some(&OwnedValue::Null)),
            expected_val
        );
    }

    #[test]
    fn test_exec_if() {
        let reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, false, false));
        assert!(exec_if(&reg, false, true));

        let reg = OwnedValue::Integer(1);
        assert!(exec_if(&reg, false, false));
        assert!(!exec_if(&reg, false, true));

        let reg = OwnedValue::Null;
        assert!(!exec_if(&reg, false, false));
        assert!(!exec_if(&reg, false, true));

        let reg = OwnedValue::Null;
        assert!(exec_if(&reg, true, false));
        assert!(exec_if(&reg, true, true));

        let reg = OwnedValue::Null;
        assert!(!exec_if(&reg, false, false));
        assert!(!exec_if(&reg, false, true));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::build_text("limbo"),
                &OwnedValue::build_text("limbo")
            ),
            OwnedValue::Null
        );

        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(2)),
            OwnedValue::Integer(1)
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.2)),
            OwnedValue::Float(1.1)
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::build_text("limbo"),
                &OwnedValue::build_text("limb")
            ),
            OwnedValue::build_text("limbo")
        );
    }

    #[test]
    fn test_substring() {
        let str_value = OwnedValue::build_text("limbo");
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::build_text("lim");
        assert_eq!(
            exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = OwnedValue::build_text("limbo");
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(10);
        let expected_val = OwnedValue::build_text("limbo");
        assert_eq!(
            exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = OwnedValue::build_text("limbo");
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::build_text("");
        assert_eq!(
            exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = OwnedValue::build_text("limbo");
        let start_value = OwnedValue::Integer(3);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::build_text("mbo");
        assert_eq!(
            exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = OwnedValue::build_text("limbo");
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::build_text("");
        assert_eq!(
            exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = OwnedValue::build_text("limbo");
        let pattern = OwnedValue::build_text("im");
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("limbo");
        let pattern = OwnedValue::build_text("limbo");
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("limbo");
        let pattern = OwnedValue::build_text("o");
        let expected = OwnedValue::Integer(5);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("liiiiimbo");
        let pattern = OwnedValue::build_text("ii");
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("limbo");
        let pattern = OwnedValue::build_text("limboX");
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("limbo");
        let pattern = OwnedValue::build_text("");
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("");
        let pattern = OwnedValue::build_text("limbo");
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("");
        let pattern = OwnedValue::build_text("");
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("limbo");
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::build_text("limbo");
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(2);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(5);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(2.3);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(5.6);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::build_text(".");
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(vec![1, 2, 3, 4, 5]);
        let pattern = OwnedValue::Blob(vec![3, 4]);
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(vec![1, 2, 3, 4, 5]);
        let pattern = OwnedValue::Blob(vec![3, 2]);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(vec![0x61, 0x62, 0x63, 0x64, 0x65]);
        let pattern = OwnedValue::build_text("cd");
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text("abcde");
        let pattern = OwnedValue::Blob(vec![0x63, 0x64]);
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);
    }

    #[test]
    fn test_exec_sign() {
        let input = OwnedValue::Integer(42);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(-42);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.1);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(42.0);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(-42.0);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text("abc");
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text("42");
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text("-42");
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text("0");
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(b"abc".to_vec());
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(b"42".to_vec());
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(b"-42".to_vec());
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(b"0".to_vec());
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Null;
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);
    }

    #[test]
    fn test_exec_zeroblob() {
        let input = OwnedValue::Integer(0);
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(4);
        let expected = OwnedValue::Blob(vec![0; 4]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(-1);
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text("5");
        let expected = OwnedValue::Blob(vec![0; 5]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text("-5");
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text("text");
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Float(2.6);
        let expected = OwnedValue::Blob(vec![0; 2]);
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Blob(vec![1]);
        let expected = OwnedValue::Blob(vec![]);
        assert_eq!(exec_zeroblob(&input), expected);
    }

    #[test]
    fn test_execute_sqlite_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_sqlite_version(version_integer), expected);
    }

    #[test]
    fn test_replace() {
        let input_str = OwnedValue::build_text("bob");
        let pattern_str = OwnedValue::build_text("b");
        let replace_str = OwnedValue::build_text("a");
        let expected_str = OwnedValue::build_text("aoa");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bob");
        let pattern_str = OwnedValue::build_text("b");
        let replace_str = OwnedValue::build_text("");
        let expected_str = OwnedValue::build_text("o");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bob");
        let pattern_str = OwnedValue::build_text("b");
        let replace_str = OwnedValue::build_text("abc");
        let expected_str = OwnedValue::build_text("abcoabc");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bob");
        let pattern_str = OwnedValue::build_text("a");
        let replace_str = OwnedValue::build_text("b");
        let expected_str = OwnedValue::build_text("bob");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bob");
        let pattern_str = OwnedValue::build_text("");
        let replace_str = OwnedValue::build_text("a");
        let expected_str = OwnedValue::build_text("bob");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bob");
        let pattern_str = OwnedValue::Null;
        let replace_str = OwnedValue::build_text("a");
        let expected_str = OwnedValue::Null;
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bo5");
        let pattern_str = OwnedValue::Integer(5);
        let replace_str = OwnedValue::build_text("a");
        let expected_str = OwnedValue::build_text("boa");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bo5.0");
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::build_text("a");
        let expected_str = OwnedValue::build_text("boa");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bo5");
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::build_text("a");
        let expected_str = OwnedValue::build_text("bo5");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text("bo5.0");
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::Float(6.0);
        let expected_str = OwnedValue::build_text("bo6.0");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
        let input_str = OwnedValue::build_text("tes3");
        let pattern_str = OwnedValue::Integer(3);
        let replace_str = OwnedValue::Float(0.3);
        let expected_str = OwnedValue::build_text("tes0.3");
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );
    }

    #[test]
    fn test_likely() {
        let input = OwnedValue::build_text("limbo");
        let expected = OwnedValue::build_text("limbo");
        assert_eq!(exec_likely(&input), expected);

        let input = OwnedValue::Integer(100);
        let expected = OwnedValue::Integer(100);
        assert_eq!(exec_likely(&input), expected);

        let input = OwnedValue::Float(12.34);
        let expected = OwnedValue::Float(12.34);
        assert_eq!(exec_likely(&input), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_likely(&input), expected);

        let input = OwnedValue::Blob(vec![1, 2, 3, 4]);
        let expected = OwnedValue::Blob(vec![1, 2, 3, 4]);
        assert_eq!(exec_likely(&input), expected);
    }

    #[test]
    fn test_likelihood() {
        let value = OwnedValue::build_text("limbo");
        let prob = OwnedValue::Float(0.5);
        assert_eq!(exec_likelihood(&value, &prob), value);

        let value = OwnedValue::build_text("database");
        let prob = OwnedValue::Float(0.9375);
        assert_eq!(exec_likelihood(&value, &prob), value);

        let value = OwnedValue::Integer(100);
        let prob = OwnedValue::Float(1.0);
        assert_eq!(exec_likelihood(&value, &prob), value);

        let value = OwnedValue::Float(12.34);
        let prob = OwnedValue::Float(0.5);
        assert_eq!(exec_likelihood(&value, &prob), value);

        let value = OwnedValue::Null;
        let prob = OwnedValue::Float(0.5);
        assert_eq!(exec_likelihood(&value, &prob), value);

        let value = OwnedValue::Blob(vec![1, 2, 3, 4]);
        let prob = OwnedValue::Float(0.5);
        assert_eq!(exec_likelihood(&value, &prob), value);

        let prob = OwnedValue::build_text("0.5");
        assert_eq!(exec_likelihood(&value, &prob), value);

        let prob = OwnedValue::Null;
        assert_eq!(exec_likelihood(&value, &prob), value);
    }

    #[test]
    fn test_bitfield() {
        let mut bitfield = Bitfield::<4>::new();
        for i in 0..256 {
            bitfield.set(i);
            assert!(bitfield.get(i));
            for j in 0..i {
                assert!(bitfield.get(j));
            }
            for j in i + 1..256 {
                assert!(!bitfield.get(j));
            }
        }
        for i in 0..256 {
            bitfield.unset(i);
            assert!(!bitfield.get(i));
        }
    }
}
