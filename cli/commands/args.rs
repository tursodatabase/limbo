use clap::{Args, ValueEnum};

use crate::input::OutputMode;

#[derive(Debug, Clone, Args)]
pub struct ExitArgs {
    /// Exit code
    #[arg(default_value_t = 0)]
    pub code: i32,
}

#[derive(Debug, Clone, Args)]
pub struct OpenArgs {
    /// Path to open database
    pub path: String,
}

#[derive(Debug, Clone, Args)]
pub struct SchemaArgs {
    /// Table name to visualize schema
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct SetOutputArgs {
    /// File path to send output to
    pub path: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct OutputModeArgs {
    #[arg(value_enum)]
    pub mode: OutputMode,
}

#[derive(Debug, Clone, Args)]
pub struct OpcodesArgs {
    /// Opcode to display description
    pub opcode: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct CwdArgs {
    /// Target directory
    pub directory: String,
}

#[derive(Debug, Clone, Args)]
pub struct NullValueArgs {
    pub value: String,
}

#[derive(Debug, Clone, Args)]
pub struct EchoArgs {
    #[arg(value_enum)]
    pub mode: EchoMode,
}

#[derive(Debug, ValueEnum, Clone)]
pub enum EchoMode {
    On,
    Off,
}

#[derive(Debug, Clone, Args)]
pub struct TablesArgs {
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct LoadExtensionArgs {
    /// Path to extension file
    pub path: String,
}
