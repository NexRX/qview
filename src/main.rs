reexport!(testing, test);
reexport!(error);
reexport!(validator);
reexport!(config);
#[allow(unused_imports)]
pub(crate) use tracing::{debug, error, info, span, trace, warn};

fn main() {
    println!("Hello, world!");
}

#[macro_export]
macro_rules! reexport {
    ($module:ident) => {
        $crate::reexport!($module, false);
    };
    ($module:ident, test) => {
        $crate::reexport!($module, true);
    };
    ($module:ident, $is_test:literal) => {
        #[cfg_attr($is_test, cfg(test))]
        mod $module;
        #[cfg_attr($is_test, cfg(test))]
        #[allow(unused_imports)]
        pub use $module::*;
    };
}
