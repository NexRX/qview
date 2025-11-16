#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    start: usize,
    end: Option<usize>,
}

impl Cursor {
    pub fn new(start: usize, end: Option<usize>) -> Self {
        Self { start, end }
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> Option<usize> {
        self.end
    }

    pub fn range(&self) -> (usize, Option<usize>) {
        (self.start, self.end)
    }
}
