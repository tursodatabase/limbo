use clap::{command, Parser};

#[derive(Parser)]
#[command(name = "limbo_stress")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[clap(short = 't', long, help = "the number of threads", default_value_t = 8)]
    pub nr_threads: usize,
    #[clap(
        short = 'i',
        long,
        help = "the number of iterations",
        default_value_t = 1000
    )]
    pub nr_iterations: usize,
}
