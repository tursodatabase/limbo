pub mod args;
pub mod import;

use args::{
    CwdArgs, EchoArgs, ExitArgs, LoadExtensionArgs, NullValueArgs, OpcodesArgs, OpenArgs,
    OutputModeArgs, SchemaArgs, SetOutputArgs, TablesArgs,
};
use clap::Parser;
use import::ImportArgs;

use crate::input::{AFTER_HELP_MSG, BEFORE_HELP_MSG};

#[derive(Parser, Debug)]
#[command(
    multicall = true,
    arg_required_else_help(false),
    before_help(BEFORE_HELP_MSG),
    after_help(AFTER_HELP_MSG)
)]
pub struct CommandParser {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, clap::Subcommand)]
#[command(disable_help_flag(false), disable_version_flag(true))]
pub enum Command {
    /// Exit this program with return-code CODE
    #[command(subcommand_value_name = ".exit")]
    Exit(ExitArgs),
    /// Quit the shell
    #[command(subcommand_value_name = ".quit")]
    Quit,
    /// Open a database file
    #[command(subcommand_value_name = ".open")]
    Open(OpenArgs),
    //  Display help message
    // Help,
    /// Display schema for a table
    #[command(subcommand_value_name = ".schema")]
    Schema(SchemaArgs),
    /// Set output file (or stdout if empty)
    #[command(name = "output", subcommand_value_name = ".output")]
    SetOutput(SetOutputArgs),
    /// Set output display mode
    #[command(
        name = "mode",
        subcommand_value_name = ".mode",
        arg_required_else_help(false)
    )]
    OutputMode(OutputModeArgs),
    /// Show vdbe opcodes
    #[command(subcommand_value_name = ".opcodes")]
    Opcodes(OpcodesArgs),
    /// Change the current working directory
    #[command(name = "cd", subcommand_value_name = ".cd")]
    Cwd(CwdArgs),
    /// Display information about settings
    #[command(name = "show")]
    ShowInfo,
    /// Set the value of NULL to be printed in 'list' mode
    #[command(name = "nullvalue", subcommand_value_name = ".nullvalue")]
    NullValue(NullValueArgs),
    /// Toggle 'echo' mode to repeat commands before execution
    #[command(subcommand_value_name = ".echo")]
    Echo(EchoArgs),
    /// Display tables
    #[command(subcommand_value_name = ".tables")]
    Tables(TablesArgs),
    /// Import data from FILE into TABLE
    #[command(subcommand_value_name = ".import")]
    Import(ImportArgs),
    /// Loads an extension library
    #[command(name = "load", subcommand_value_name = ".load")]
    LoadExtension(LoadExtensionArgs),
    /// Dump the current database as a list of SQL statements
    #[command(subcommand_value_name = ".dump")]
    Dump,
}

// impl Command {
//     pub fn min_args(&self) -> usize {
//         1 + match self {
//             Self::Exit
//             | Self::Quit
//             | Self::Schema
//             | Self::Help
//             | Self::Opcodes
//             | Self::ShowInfo
//             | Self::Tables
//             | Self::SetOutput
//             | Self::Dump => 0,
//             Self::Open
//             | Self::OutputMode
//             | Self::Cwd
//             | Self::Echo
//             | Self::NullValue
//             | Self::LoadExtension => 1,
//             Self::Import => 2,
//         }
//     }

//     pub fn usage(&self) -> &str {
//         match self {
//             Self::Exit => ".exit ?<CODE>",
//             Self::Quit => ".quit",
//             Self::Open => ".open <file>",
//             Self::Help => ".help",
//             Self::Schema => ".schema ?<table>?",
//             Self::Opcodes => ".opcodes",
//             Self::OutputMode => ".mode list|pretty",
//             Self::SetOutput => ".output ?file?",
//             Self::Cwd => ".cd <directory>",
//             Self::ShowInfo => ".show",
//             Self::NullValue => ".nullvalue <string>",
//             Self::Echo => ".echo on|off",
//             Self::Tables => ".tables",
//             Self::LoadExtension => ".load",
//             Self::Dump => ".dump",
//             Self::Import => &IMPORT_HELP,
//         }
//     }
// }

// impl FromStr for Command {
//     type Err = String;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s {
//             ".exit" => Ok(Self::Exit),
//             ".quit" => Ok(Self::Quit),
//             ".open" => Ok(Self::Open),
//             ".help" => Ok(Self::Help),
//             ".schema" => Ok(Self::Schema),
//             ".tables" => Ok(Self::Tables),
//             ".opcodes" => Ok(Self::Opcodes),
//             ".mode" => Ok(Self::OutputMode),
//             ".output" => Ok(Self::SetOutput),
//             ".cd" => Ok(Self::Cwd),
//             ".show" => Ok(Self::ShowInfo),
//             ".nullvalue" => Ok(Self::NullValue),
//             ".echo" => Ok(Self::Echo),
//             ".import" => Ok(Self::Import),
//             ".load" => Ok(Self::LoadExtension),
//             ".dump" => Ok(Self::Dump),
//             _ => Err("Unknown command".to_string()),
//         }
//     }
// }
