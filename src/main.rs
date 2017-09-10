use std::io;
use std::io::prelude::*;

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

enum MetaCommand {
    Exit
}

fn do_meta_command(command: &str) -> Result<MetaCommand, ParseError> {
    if command.starts_with(".exit") {
        Ok(MetaCommand::Exit)
    } else {
        Err(ParseError::Unrecognized)
    }
}

enum ParseError {
    Unrecognized
}

enum Statement {
    Insert, Select
}

fn prepare_statement(input: &str) -> Result<Statement, ParseError> {
    if input.starts_with("insert") {
        Ok(Statement::Insert)
    }  else if input.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(ParseError::Unrecognized)
    }
}

fn execute_statement(statement: Statement) {
  match statement {
    Statement::Insert => println!("This is where we would do an insert."),
    Statement::Select => println!("This is where we would do a select.")
  }
}

fn main() {
    println!("Hello, world!");
    let stdin = io::stdin();
    let lines = stdin.lock().lines();

    print_prompt();
    for line in lines {
        if let Ok(input) = line {
            if let Some('.') = input.chars().next() {
                match do_meta_command(&input) {
                    Ok(MetaCommand::Exit) => break,
                    Err(ParseError::Unrecognized) => println!("Unrecognized command '{}'", input)
                }
            }
            
            match prepare_statement(&input) {
                Ok(statement) => {
                    execute_statement(statement);
                    println!("Executed.")
                }
                Err(ParseError::Unrecognized) => println!("Unrecognized keyword at start of {}", input),
            }
        }
        print_prompt();
    }
}
