#![feature(const_size_of)]
extern crate byteorder;

use std::io;
use std::io::prelude::*;
use std::cmp;
use std::fmt;
use std::str;
use std::env;
use std::fs::{File, OpenOptions};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};

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
const ROW_SIZE: usize = std::mem::size_of::<Row>();

#[derive(Debug)]
enum PagerError {
    OutOfBounds { page_num: usize, max_pages: usize },
    CouldNotRead,
}

struct Pager {
    // f: io::BufReader<File>,
    fd: File,
    file_size: u64,
    pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
}

impl Pager {
    fn new(f: File, file_size: u64) -> Self {
        Pager {
            fd: f,
            file_size,
            pages: {
                let mut array: [Option<Box<Page>>; 100] = unsafe { std::mem::uninitialized() };
                for i in array.iter_mut() {
                    unsafe {
                        ::std::ptr::write(i, None);
                    }
                }
                array
            },
        }
    }

    fn open(filename: &str) -> Result<Box<Self>, io::Error> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;
        let file_size = f.seek(io::SeekFrom::End(0))
            .expect("Could not read to end of file");
        let pager = Box::new(Pager::new(f, file_size));
        Ok(pager)
    }

    fn read_into_row<R: Read>(&self, reader: &mut R, row: &mut Row) -> io::Result<()> {
        row.id = reader.read_u32::<LE>()?;
        row.username_len = reader.read_u32::<LE>()?;
        reader.read(&mut row.username)?;
        row.email_len = reader.read_u32::<LE>()?;
        reader.read(&mut row.email)?;
        Ok(())
    }

    fn get_page<'a>(&'a mut self, page_num: usize) -> Result<&'a Page, PagerError> {
        if page_num > TABLE_MAX_PAGES {
            Err(PagerError::OutOfBounds {
                page_num,
                max_pages: TABLE_MAX_PAGES,
            })
        } else {
            match self.pages[page_num] {
                Some(ref page) => Ok(page),
                None => {
                    let mut new_page = self.allocate_page(page_num as u64).unwrap();
                        // .map_err(|_| PagerError::CouldNotRead)?;
                    self.pages[page_num] = Some(new_page);
                    Ok(self.pages[page_num].as_ref().unwrap())
                }
            }
        }
    }

    fn allocate_page(&mut self, page_num: u64) -> io::Result<Box<Page>> {
        let mut new_page = Box::new(Page::new());
        let num_pages = self.file_size / PAGE_SIZE + ((self.file_size % PAGE_SIZE != 0) as u64);

        if (page_num as u64) < num_pages {
            self.fd.seek(io::SeekFrom::Start(page_num as u64 * PAGE_SIZE))?;
            let mut row_bytes = [0u8; ROW_SIZE];
            for row in new_page.rows.iter_mut() {
                match self.fd.read(&mut row_bytes) {
                    Ok(ROW_SIZE) => self.read_into_row(&mut row_bytes.as_ref(), row)?,
                    Ok(0) => break,
                    Ok(_) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                    Err(e) => return Err(e)
                }
            }
        }
        Ok(new_page)
    }

    fn get_page_mut<'a>(&'a mut self, page_num: usize) -> Result<&'a mut Page, PagerError> {
        match self.pages[page_num] {
            Some(ref mut page) => Ok(page),
            None => {
                let mut new_page = self.allocate_page(page_num as u64)
                    .map_err(|_| PagerError::CouldNotRead)?;
                self.pages[page_num] = Some(new_page);
                Ok(self.pages[page_num].as_mut().unwrap())
                // match self.pages[page_num as usize] {
                //     Some(ref mut page) => page.borrow_mut(),
                //     _ => unreachable!(),
                // }
            }
        }
    }

    // FIXME get rid of this
    fn flush_partial(&mut self, page_num: usize, rows: usize) -> Result<(), io::Error> {
        if let Some(ref page) = self.pages[page_num] {
            let mut out = io::BufWriter::with_capacity(ROW_SIZE * rows, &mut self.fd);
            out.seek(io::SeekFrom::Start(page_num as u64 * PAGE_SIZE))
                .unwrap();
            for r in page.rows[..rows].iter() {
                out.write_u32::<LE>(r.id)?;
                out.write_u32::<LE>(r.username_len)?;
                out.write(&r.username)?;
                out.write_u32::<LE>(r.email_len)?;
                out.write(&r.email)?;
            }
        }
        Ok(())
    }

    fn flush_page(&mut self, page_num: usize) -> Result<(), io::Error> {
        let mut out = io::BufWriter::with_capacity(PAGE_SIZE as usize, &mut self.fd);
        if let Some(ref page) = self.pages[page_num] {
            out.seek(io::SeekFrom::Start(page_num as u64 * PAGE_SIZE))
                .unwrap();
            for r in page.rows.iter() {
                out.write_u32::<LE>(r.id)?;
                out.write_u32::<LE>(r.username_len)?;
                out.write(&r.username)?;
                out.write_u32::<LE>(r.email_len)?;
                out.write(&r.email)?;
            }
        }
        Ok(())
    }
}

// impl Drop for Pager {
//     fn drop(&mut self) {
//         for i in 0..self.pages.len() {}
//     }
// }

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
        row.username[..username_len].copy_from_slice(&username[..username_len]);
        row.email[..email_len].copy_from_slice(&email[..email_len]);
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
const PAGE_SIZE: u64 = std::mem::size_of::<Page>() as u64;

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
    pager: Box<Pager>,
    num_rows: u32,
}

impl Table {
    fn new(pager: Box<Pager>) -> Self {
        Table {
            num_rows: (pager.file_size / ROW_SIZE as u64) as u32,
            pager,
        }
    }

    fn row_slot<'a>(&'a mut self, row_num: usize) -> &'a Row {
        let (page_num, row_offset) = (row_num / ROWS_PER_PAGE, row_num % ROWS_PER_PAGE);
        let page = self.pager.get_page(page_num).unwrap();
        page.get_row(row_offset)
    }

    fn insert_row(&mut self, row: &Row, row_num: usize) -> Result<(), ExecuteError> {
        if self.num_rows >= TABLE_MAX_ROWS {
            Err(ExecuteError::TableFull)
        } else {
            let (page_num, row_offset) = (row_num / ROWS_PER_PAGE, row_num % ROWS_PER_PAGE);
            let page = self.pager.get_page_mut(page_num).unwrap();
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
                let id = id_str
                    .parse::<i32>()
                    .map_err(|_| ParseError::InvalidSyntax)?;
                if id < 0 {
                    return Err(ParseError::NegativeID);
                }
                let (uname_bytes, email_bytes) = (username.as_bytes(), email.as_bytes());
                if uname_bytes.len() > MAX_UNAME_LENGTH || email_bytes.len() > MAX_EMAIL_LENGTH {
                    return Err(ParseError::StringTooLong);
                }
                Ok(Statement::Insert(
                    Row::new(id as u32, uname_bytes, email_bytes),
                ))
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
    table.insert_row(row, num_rows)?;
    table.num_rows += 1;
    Ok(())
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

fn db_open(filename: &str) -> Result<Table, io::Error> {
    let pager = Pager::open(filename)?;
    let table = Table::new(pager);
    Ok(table)
}

fn db_close(table: &mut Table) -> Result<(), io::Error> {
    let full_pages = table.num_rows as usize / ROWS_PER_PAGE;
    for i in 0..full_pages {
        table.pager.flush_page(i)?;
    }
    let remaining_rows = table.num_rows as usize % ROWS_PER_PAGE;
    if remaining_rows > 0 {
        table.pager.flush_partial(full_pages, remaining_rows)?;
    }
    Ok(())
}

fn main() {
    let mut args = env::args();
    let filename = match (args.next(), args.next()) {
        (_, None) => {
            println!("Must supply a database filename.");
            std::process::exit(1);
        }
        (_, Some(filename)) => filename,
    };

    let mut table = match db_open(&filename) {
        Ok(t) => t,
        Err(e) => {
            println!("Could not open file {}: {}", filename, e);
            std::process::exit(1);
        }
    };

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
                },
                Err(ParseError::Unrecognized) => {
                    println!("Unrecognized keyword at start of {}", input)
                }
                Err(ParseError::NegativeID) => println!("ID must be positive."),
                Err(ParseError::StringTooLong) => println!("String is too long."),
                Err(ParseError::InvalidSyntax) => {
                    println!("Syntax error: could not parse statement.")
                }
            }
        }
        print_prompt();
    }
    if let Err(e) = db_close(&mut table) {
        println!("Could not close db {}: {}", filename, e);
    }
}
