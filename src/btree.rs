use serde_ext::BigArray;

use std::{cmp, fmt, mem, str};

const MAX_UNAME_LENGTH: usize = 32;
const MAX_EMAIL_LENGTH: usize = 255;
pub const PAGE_SIZE: usize = 4096;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct Row {
    pub id: u32,
    username_len: u8,
    username: [u8; MAX_UNAME_LENGTH as usize],
    email_len: u8,
    #[serde(with = "BigArray")] email: [u8; MAX_EMAIL_LENGTH as usize],
    // _pad: [u8; 3],
}

impl Row {
    pub fn new(id: u32, username: &[u8], email: &[u8]) -> Self {
        let username_len = cmp::min(MAX_UNAME_LENGTH, username.len());
        let email_len = cmp::min(MAX_EMAIL_LENGTH, email.len());
        let mut row = Row {
            id: id,
            username_len: username_len as u8,
            username: [0; MAX_UNAME_LENGTH as usize],
            email_len: email_len as u8,
            email: [0; MAX_EMAIL_LENGTH as usize],
            // _pad: Default::default(),
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
            // _pad: Default::default(),
        }
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

#[derive(Default, Serialize, Deserialize)]
pub struct Cell(pub u32, pub Row);

impl Cell {
    pub fn set_key(&mut self, key: u32) {
        self.0 = key;
    }
    pub fn set_val(&mut self, val: &Row) {
        self.1 = *val;
    }
}


#[derive(Default, Serialize, Deserialize)]
pub struct NodeHeader {
    is_root: bool,
    parent_ptr: u32,
}

pub const LEAF_NODE_MAX_CELLS: usize =
    (PAGE_SIZE - mem::size_of::<NodeHeader>() - 4) / mem::size_of::<Cell>();

#[derive(Serialize, Deserialize)]
pub enum Node {
    Leaf {
        header: NodeHeader,
        num_cells: u32,
        cells: [Cell; LEAF_NODE_MAX_CELLS],
    },
}

impl Node {
    pub fn create_leaf() -> Self {
        Node::Leaf {
            header: Default::default(),
            cells: Default::default(),
            num_cells: 0,
        }
    }

    pub fn insert(&mut self, cell_num: u32, key: u32, val: &Row) {
        match self {
            &mut Node::Leaf {
                ref mut num_cells,
                ref mut cells,
                ..
            } => {
                let cell_count = *num_cells as usize;
                let insert_idx = cell_num as usize;
                if cell_count >= LEAF_NODE_MAX_CELLS {
                    unimplemented!("Splitting leaf nodes");
                }
                if insert_idx < cell_count {
                    let (start, end) = (cell_num as usize, cell_count as usize);
                    for i in (start + 1..end + 1).rev() {
                        let (left, right) = cells.split_at_mut(i);
                        mem::swap(&mut left[i - 1], &mut right[0]);
                    }
                }
                let c = &mut cells[insert_idx];
                c.set_val(val);
                c.set_key(key);
                *num_cells += 1;
            }
        }
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Node::Leaf {
                num_cells,
                ref cells,
                ..
            } => {
                writeln!(f, "leaf (size {})", num_cells)?;
                for (i, &Cell(key, ..)) in cells[..num_cells as usize].iter().enumerate() {
                    writeln!(f, "  - {} : {}", i, key)?;
                }
                Ok(())
            }
        }
    }
}
