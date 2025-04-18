#![allow(clippy::arc_with_non_send_sync)]
mod app;
mod commands;
mod config;
mod helper;
mod input;
mod opcodes_dictionary;

use config::CONFIG_DIR;
use once_cell::sync::Lazy;
use rustyline::{error::ReadlineError, Config, Editor};
use std::{path::PathBuf, sync::atomic::Ordering};

fn rustyline_config() -> Config {
    Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .build()
}

pub static HOME_DIR: Lazy<PathBuf> =
    Lazy::new(|| dirs::home_dir().expect("Could not determine home directory"));

pub static HISTORY_FILE: Lazy<PathBuf> = Lazy::new(|| HOME_DIR.join(".limbo_history"));

fn main() -> anyhow::Result<()> {
    let mut rl = Editor::with_config(rustyline_config())?;

    // let home = dirs::home_dir().expect("Could not determine home directory");
    let config_file = CONFIG_DIR.join("limbo.toml");

    let config = config::Config::from_config_file(config_file);
    tracing::info!("Configuration: {:?}", config);

    let mut app = app::Limbo::new(&mut rl, &config)?;

    let _guard = app.init_tracing()?;
    if HISTORY_FILE.exists() {
        app.rl.load_history(HISTORY_FILE.as_path())?;
    }
    loop {
        let readline = app.rl.readline(&app.prompt);
        match readline {
            Ok(line) => match app.handle_input_line(line.trim()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{}", e);
                }
            },
            Err(ReadlineError::Interrupted) => {
                // At prompt, increment interrupt count
                if app.interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    let _ = app.close_conn();
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                app.reset_input();
                continue;
            }
            Err(ReadlineError::Eof) => {
                app.handle_remaining_input();
                let _ = app.close_conn();
                break;
            }
            Err(err) => {
                let _ = app.close_conn();
                anyhow::bail!(err)
            }
        }
    }
    app.rl.save_history(HISTORY_FILE.as_path())?;
    Ok(())
}
