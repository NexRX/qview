#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    start: usize,
    end: Option<usize>,
}

impl Cursor {
    pub fn new(start: usize, end: Option<usize>) -> Self {
        Self { start, end }
    }
}
