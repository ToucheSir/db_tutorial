use std::io;
use std::io::prelude::*;

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn main() {
    println!("Hello, world!");
    let stdin = io::stdin();
    let lines = stdin.lock().lines();

    print_prompt();
    for line in lines {
        if let Ok(input) = line {
            if input.starts_with(".exit") {
                break
            } else {
                println!("Unrecognized command '{}'", input);
            }
        }
        print_prompt();
    }
}
