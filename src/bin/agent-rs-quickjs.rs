//! The script process of `agent_rs::codemode::Subprocess`: serves one QuickJS session over
//! standard input and output, then exits.
fn main() {
    let (stdin, stdout) = (std::io::stdin(), std::io::stdout());
    if let Err(error) = agent_rs::codemode::serve(stdin.lock(), stdout.lock()) {
        eprintln!("agent-rs-quickjs: {error}");
        std::process::exit(1);
    }
}
