#![feature(const_size_of)]
use std::io;
use std::io::prelude::*;
use std::cmp;
use std::fmt;
use std::str;

const MAX_UNAME_LENGTH: usize = 32;
const MAX_EMAIL_LENGTH: usize = 256;

#[derive(Copy)]
struct Row {
    id: u32,
    username_len: u32,
    username: [u8; MAX_UNAME_LENGTH],
    email_len: u32,
    email: [u8; MAX_EMAIL_LENGTH],
}

impl Row {
    fn new(id: u32, username: &[u8], email: &[u8]) -> Self {
        let username_len = cmp::min(MAX_UNAME_LENGTH, username.len());
        let email_len = cmp::min(MAX_EMAIL_LENGTH, email.len());
        let mut row = Row {
            id: id,
            username_len: username_len as u32,
            username: [0; MAX_UNAME_LENGTH],
            email_len: email_len as u32,
            email: [0; MAX_EMAIL_LENGTH],
        };
        row.username[..username_len].clone_from_slice(&username[..username_len]);
        row.email[..email_len].clone_from_slice(&email[..email_len]);
        row
    }
}

impl Default for Row {
    fn default() -> Self {
        Row {
            id: 0,
            username_len: 0,
            username: [0; MAX_UNAME_LENGTH],
            email_len: 0,
            email: [0; MAX_EMAIL_LENGTH],
        }
    }
}

// Hack because not all fixed-size array struct members are `Clone`
impl Clone for Row {
    fn clone(&self) -> Self {
        *self
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (
            str::from_utf8(&self.username[..self.username_len as usize]),
            str::from_utf8(&self.email[..self.email_len as usize]),
        ) {
            (Ok(username), Ok(email)) => write!(f, "({}, {}, {})", self.id, username, email),
            _ => Err(fmt::Error),
        }
    }
}

const ROWS_PER_PAGE: usize = 4096 / std::mem::size_of::<Row>();
const TABLE_MAX_PAGES: usize = 100;
const TABLE_MAX_ROWS: u32 = (ROWS_PER_PAGE * TABLE_MAX_PAGES) as u32;

#[derive(Copy, Clone)]
struct Page {
    rows: [Row; ROWS_PER_PAGE],
}

impl Page {
    fn new() -> Self {
        Page {
            rows: [Default::default(); ROWS_PER_PAGE],
        }
    }

    fn get_row<'a>(&'a self, row_num: usize) -> &'a Row {
        &self.rows[row_num]
    }

    fn set_row<'a>(&'a mut self, row_num: usize, row: &Row) {
        self.rows[row_num] = *row;
    }
}

struct Table {
    pages: [Option<Box<Page>>; 100],
    num_rows: u32,
}

impl Table {
    fn new() -> Self {
        Table {
            pages: {
                let mut array: [Option<Box<Page>>; 100] = unsafe { std::mem::uninitialized() };
                for i in array.iter_mut() {
                    unsafe {
                        ::std::ptr::write(i, None);
                    }
                }
                array
            },
            num_rows: 0,
        }
    }

    fn get_page<'a>(&'a mut self, page_num: usize) -> &'a Page {
        match self.pages[page_num] {
            Some(ref page) => page,
            None => {
                let new_page = Box::new(Page::new());
                self.pages[page_num] = Some(new_page);
                self.pages[page_num].as_ref().unwrap()
            }
        }
    }

    fn get_page_mut<'a>(&'a mut self, page_num: usize) -> &'a mut Page {
        match self.pages[page_num] {
            Some(ref mut page) => page,
            None => {
                let new_page = Box::new(Page::new());
                self.pages[page_num] = Some(new_page);
                self.pages[page_num].as_mut().unwrap()
                // match self.pages[page_num as usize] {
                //     Some(ref mut page) => page.borrow_mut(),
                //     _ => unreachable!(),
                // }
            }
        }
    }

    fn row_slot<'a>(&'a mut self, row_num: usize) -> &'a Row {
        let (page_num, row_offset) = (row_num / ROWS_PER_PAGE, row_num % ROWS_PER_PAGE);
        let page = self.get_page(page_num);
        page.get_row(row_offset)
    }

    fn insert_row(&mut self, row: &Row, row_num: usize) -> Result<(), ExecuteError> {
        if self.num_rows >= TABLE_MAX_ROWS {
            Err(ExecuteError::TableFull)
        } else {
            let (page_num, row_offset) = (row_num / ROWS_PER_PAGE, row_num % ROWS_PER_PAGE);
            let page = self.get_page_mut(page_num);
            page.set_row(row_offset, row);
            Ok(())
        }
    }
}

enum MetaCommand {
    Exit,
}

fn do_meta_command(command: &str) -> Result<MetaCommand, ParseError> {
    if command.starts_with(".exit") {
        Ok(MetaCommand::Exit)
    } else {
        Err(ParseError::Unrecognized)
    }
}

enum ParseError {
    Unrecognized,
    InvalidSyntax,
    StringTooLong,
    NegativeID,
}

enum Statement {
    Insert(Row),
    Select,
}

fn prepare_statement(input: &str) -> Result<Statement, ParseError> {
    if input.starts_with("insert") {
        let mut tokens = input.split_whitespace();
        let _ = tokens.next(); // skip "insert"
        match (tokens.next(), tokens.next(), tokens.next()) {
            (Some(id_str), Some(username), Some(email)) => {
                let id = id_str.parse::<i32>().map_err(|_| ParseError::InvalidSyntax)?;
                if id < 0 { return Err(ParseError::NegativeID) }
                let (uname_bytes, email_bytes) = (username.as_bytes(), email.as_bytes());
                if uname_bytes.len() > MAX_UNAME_LENGTH || email_bytes.len() > MAX_EMAIL_LENGTH {
                    return Err(ParseError::StringTooLong)
                } 
                Ok(Statement::Insert(Row::new(id as u32, uname_bytes, email_bytes)))
            }
            _ => Err(ParseError::InvalidSyntax),
        }
    } else if input.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(ParseError::Unrecognized)
    }
}

enum ExecuteError {
    TableFull,
}

fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecuteError> {
    let num_rows = table.num_rows as usize;
    let res = table.insert_row(row, num_rows);
    table.num_rows += 1;
    res
}

fn execute_select(table: &mut Table) -> Result<(), ExecuteError> {
    for i in 0..table.num_rows {
        println!("{}", table.row_slot(i as usize));
    }
    Ok(())
}

fn execute_statement(statement: Statement, table: &mut Table) -> Result<(), ExecuteError> {
    match statement {
        Statement::Insert(row) => execute_insert(&row, table),
        Statement::Select => execute_select(table),
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn main() {
    let mut table = Table::new();

    let stdin = io::stdin();
    let lines = stdin.lock().lines();

    print_prompt();
    for line in lines {
        if let Ok(input) = line {
            if let Some('.') = input.chars().next() {
                match do_meta_command(&input) {
                    Ok(MetaCommand::Exit) => break,
                    Err(ParseError::Unrecognized) => println!("Unrecognized command '{}'", input),
                    _ => {}
                }
            }

            match prepare_statement(&input) {
                Ok(statement) => match execute_statement(statement, &mut table) {
                    Ok(()) => println!("Executed."),
                    Err(ExecuteError::TableFull) => println!("Error: Table full."),
                }
                Err(ParseError::Unrecognized) => {
                    println!("Unrecognized keyword at start of {}", input)
                }
                Err(ParseError::NegativeID) => {
                    println!("ID must be positive.")
                }
                Err(ParseError::StringTooLong) => {
                    println!("String is too long.")
                }
                Err(ParseError::InvalidSyntax) => {
                    println!("Syntax error: could not parse statement.")
                }
            }
        }
        print_prompt();
    }
}
