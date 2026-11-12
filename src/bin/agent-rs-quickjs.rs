//! The script process of `agent_rs::codemode::Subprocess`: serves one QuickJS session over
//! standard input and output, then exits.
fn main() {
    if let Err(error) = agent_rs::codemode::serve(std::io::stdin(), std::io::stdout().lock()) {
        eprintln!("agent-rs-quickjs: {error}");
        std::process::exit(1);
    }
}
