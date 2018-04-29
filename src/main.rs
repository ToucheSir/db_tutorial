#![feature(const_size_of)]

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;

mod btree;
mod serde_ext;

use std::env;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::mem::{align_of, size_of};

use bincode::{deserialize_from, serialize_into, Infinite};

use btree::{Node, Row, PAGE_SIZE};

const MAX_UNAME_LENGTH: usize = 32;
const MAX_EMAIL_LENGTH: usize = 255;

#[derive(Debug)]
enum PagerError {
    OutOfBounds { page_num: usize, max_pages: usize },
    CouldNotRead,
}

struct Cursor<'a> {
    table: &'a mut Table,
    page_num: u32,
    cell_num: u32,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    fn get_value(&mut self) -> &Row {
        let page_num = self.page_num as usize;
        let page = self.table.pager.get_page(page_num).unwrap();
        match page {
            &Node::Leaf { ref cells, .. } => &(cells[self.cell_num as usize].1),
            _ => unimplemented!("Internal node"),
        }
    }

    fn set_value(&mut self, val: &Row) {
        let page_num = self.page_num as usize;
        let page = self.table.pager.get_page_mut(page_num).unwrap();
        match page {
            &mut Node::Leaf { ref mut cells, .. } => {
                cells[self.cell_num as usize].set_val(val);
            }
            _ => unimplemented!("Internal node"),
        }
    }

    fn advance(&mut self) {
        let page_num = self.page_num as usize;
        let node = self.table.pager.get_page(page_num).unwrap();
        self.cell_num += 1;
        match node {
            &Node::Leaf { num_cells, .. } if self.cell_num >= num_cells => {
                self.end_of_table = true;
            }
            _ => {}
        }
    }

    fn insert(&mut self, key: u32, val: &Row) {
        let page_num = self.page_num as usize;
        let page = self.table.pager.get_page_mut(page_num).unwrap();
        page.insert(self.cell_num, key, val);
    }
}

struct Pager {
    fd: File,
    file_size: u64,
    pages: [Option<Box<Node>>; TABLE_MAX_PAGES],
    num_pages: usize,
}

impl Pager {
    fn new(f: File, file_size: u64) -> Self {
        Pager {
            fd: f,
            file_size,
            pages: {
                let mut array: [Option<Box<Node>>; 100] = unsafe { std::mem::uninitialized() };
                for i in array.iter_mut() {
                    unsafe {
                        ::std::ptr::write(i, None);
                    }
                }
                array
            },
            num_pages: file_size as usize / PAGE_SIZE,
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

    // fn read_into_row<R: Read>(&self, reader: &mut R, row: &mut Row) -> io::Result<()> {
    //     std::mem::replace(row, deserialize_from(reader, Infinite));
    //     Ok(())
    // }

    fn get_page<'a>(&'a mut self, page_num: usize) -> Result<&'a Node, PagerError> {
        if page_num > TABLE_MAX_PAGES {
            Err(PagerError::OutOfBounds {
                page_num,
                max_pages: TABLE_MAX_PAGES,
            })
        } else {
            match self.pages[page_num] {
                Some(ref page) => {
                    let _page = page;
                    Ok(page)
                }
                None => {
                    let mut new_page = self.allocate_page(page_num)
                        .map_err(|_| PagerError::CouldNotRead)?;
                    self.pages[page_num] = Some(new_page);
                    if page_num >= self.num_pages {
                        self.num_pages = page_num + 1;
                    }
                    Ok(self.pages[page_num].as_ref().unwrap())
                }
            }
        }
    }

    fn get_page_mut<'a>(&'a mut self, page_num: usize) -> Result<&'a mut Node, PagerError> {
        match self.pages[page_num] {
            Some(ref mut page) => Ok(page),
            None => {
                let mut new_page = self.allocate_page(page_num)
                    .map_err(|_| PagerError::CouldNotRead)?;
                self.pages[page_num] = Some(new_page);
                if page_num >= self.num_pages {
                    self.num_pages = page_num + 1;
                }
                Ok(self.pages[page_num].as_mut().unwrap())
                // match self.pages[page_num as usize] {
                //     Some(ref mut page) => page.borrow_mut(),
                //     _ => unreachable!(),
                // }
            }
        }
    }

    fn allocate_page(&mut self, page_num: usize) -> io::Result<Box<Node>> {
        let num_pages = self.file_size as usize / PAGE_SIZE
            + ((self.file_size as usize % PAGE_SIZE != 0) as usize);

        Ok(if page_num < num_pages {
            self.fd
                .seek(io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
            deserialize_from(&mut self.fd, Infinite).unwrap()
        } else {
            Box::new(Node::create_leaf())
        })
    }

    fn flush_page(&mut self, page_num: usize) -> Result<(), io::Error> {
        if let Some(ref page) = self.pages[page_num] {
            self.fd
                .seek(io::SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))?;
            serialize_into(&mut self.fd, &page, Infinite)
                .map_err(|_| io::Error::from(io::ErrorKind::Other))?;
        }
        Ok(())
    }
}

const TABLE_MAX_PAGES: usize = 100;

struct Table {
    pager: Box<Pager>,
    root_page_num: u32,
}

impl<'a> Table {
    fn new(pager: Box<Pager>) -> Self {
        Table {
            root_page_num: 0,
            pager,
        }
    }

    fn find(&mut self, key: u32) -> Box<Cursor> {
        let root_page_num = self.root_page_num;
        let index = {
            if let Node::Leaf {
                num_cells, cells, ..
            } = self.pager.get_page(root_page_num as usize).unwrap()
            {
                match cells[..*num_cells as usize]
                    .binary_search_by_key(&key, |&btree::Cell(k, _)| k)
                {
                    Ok(idx) => idx,
                    Err(idx) => idx,
                }
            } else {
                unimplemented!("Can't search internal nodes yet")
            }
        };
        Box::new(Cursor {
            table: self,
            page_num: root_page_num,
            cell_num: index as u32,
            end_of_table: false,
        })
    }

    fn start(&mut self) -> Box<Cursor> {
        let page_num = self.root_page_num;
        let end_of_table = match self.pager.get_page(page_num as usize).unwrap() {
            &Node::Leaf { num_cells, .. } => num_cells == 0,
            _ => unimplemented!("Internal node"),
        };
        Box::new(Cursor {
            table: self,
            page_num,
            cell_num: 0,
            end_of_table,
        })
    }

    fn end(&mut self) -> Box<Cursor> {
        let page_num = self.root_page_num;
        let cell_num = match self.pager.get_page(self.root_page_num as usize).unwrap() {
            &Node::Leaf { num_cells, .. } => num_cells,
            _ => unimplemented!("Internal node"),
        };
        Box::new(Cursor {
            table: self,
            page_num,
            cell_num,
            end_of_table: true,
        })
    }
}

enum MetaCommand {
    Exit,
    PrintConstants,
    PrintTree,
}

fn do_meta_command(command: &str) -> Result<MetaCommand, ParseError> {
    if command.starts_with(".exit") {
        Ok(MetaCommand::Exit)
    } else if command.starts_with(".constants") {
        Ok(MetaCommand::PrintConstants)
    } else if command.starts_with(".btree") {
        Ok(MetaCommand::PrintTree)
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
                Ok(Statement::Insert(Row::new(
                    id as u32,
                    uname_bytes,
                    email_bytes,
                )))
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
    DuplicateKey,
    TableFull,
}

fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecuteError> {
    use btree::LEAF_NODE_MAX_CELLS;
    let num_cells = match table.pager.get_page(table.root_page_num as usize).unwrap() {
        &Node::Leaf { num_cells, .. } => num_cells as usize,
        _ => unimplemented!("Internal node"),
    };
    if num_cells >= LEAF_NODE_MAX_CELLS {
        return Err(ExecuteError::TableFull);
    }
    {
        let key_to_insert = row.id;
        let mut cursor = table.find(key_to_insert);
        if cursor.get_value().id == key_to_insert {
            return Err(ExecuteError::DuplicateKey);
        }
        cursor.insert(key_to_insert, row);
    }
    Ok(())
}

fn execute_select(table: &mut Table) -> Result<(), ExecuteError> {
    let mut cursor = table.start();
    while !cursor.end_of_table {
        println!("{}", cursor.get_value());
        cursor.advance();
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

fn print_constants() {
    // TODO should we change these to better reflect our impl?
    // should we do repr(C) or repr(packed) for our structs as well?
    let row_size: usize = size_of::<Row>() - 3;
    let common_node_header_size: usize = size_of::<u8>() + size_of::<bool>() + size_of::<u32>();
    let leaf_node_header_size: usize = common_node_header_size + size_of::<u32>();
    let leaf_node_cell_size: usize = size_of::<btree::Cell>() - 3;
    let leaf_node_space_for_cells: usize = PAGE_SIZE - leaf_node_header_size;
    println!("ROW_SIZE: {}", row_size);
    println!("COMMON_NODE_HEADER_SIZE: {}", common_node_header_size);
    println!("LEAF_NODE_HEADER_SIZE: {}", leaf_node_header_size);
    println!("LEAF_NODE_CELL_SIZE: {}", leaf_node_cell_size);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", leaf_node_space_for_cells);
    println!("LEAF_NODE_MAX_CELLS: {}", btree::LEAF_NODE_MAX_CELLS);
}

fn db_open(filename: &str) -> Result<Table, io::Error> {
    let mut pager = Pager::open(filename)?;
    if pager.num_pages == 0 {
        let _ = pager.get_page(0);
    }
    let table = Table::new(pager);
    Ok(table)
}

fn db_close(table: &mut Table) -> Result<(), io::Error> {
    for i in 0..table.pager.num_pages {
        table.pager.flush_page(i)?;
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
                    Ok(MetaCommand::PrintConstants) => {
                        println!("Constants:");
                        print_constants();
                    }
                    Ok(MetaCommand::PrintTree) => {
                        println!("Tree:");
                        print!("{:?}", table.pager.get_page(0).unwrap());
                    }
                    Err(ParseError::Unrecognized) => println!("Unrecognized command '{}'", input),
                    _ => {}
                }
            } else {
                match prepare_statement(&input) {
                    Ok(statement) => match execute_statement(statement, &mut table) {
                        Ok(()) => println!("Executed."),
                        Err(ExecuteError::TableFull) => println!("Error: Table full."),
                        Err(ExecuteError::DuplicateKey) => println!("Error: Duplicate key."),
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
        }
        print_prompt();
    }
    if let Err(e) = db_close(&mut table) {
        println!("Could not close db {}: {}", filename, e);
    }
}
