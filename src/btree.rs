use serde_ext::BigArray;

use std::{cmp, fmt, mem, str};

const MAX_UNAME_LENGTH: usize = 32;
const MAX_EMAIL_LENGTH: usize = 255;
const PAGE_SIZE: usize = 4096;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct Row {
    id: u32,
    username_len: u8,
    username: [u8; MAX_UNAME_LENGTH as usize],
    email_len: u8,
    #[serde(with = "BigArray")] email: [u8; MAX_EMAIL_LENGTH as usize],
    _pad: [u8; 3],
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
            _pad: Default::default(),
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
            _pad: Default::default(),
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

struct Cell(u32, Row);

struct NodeHeader {
    is_root: bool,
    parent_ptr: u32,
}

enum Node {
    Leaf {
        header: NodeHeader,
        num_cells: u32,
        cells: [Cell; (PAGE_SIZE - mem::size_of::<NodeHeader>() - 4) / mem::size_of::<Cell>()],
    },
}
