use confique::Config as _;
use std::sync::OnceLock;

#[derive(confique::Config)]
pub struct Config {
    #[cfg(test)]
    #[config(env = "QVIEW_CONTAINER_RAMDISKED", default = true)]
    pub container_ramdisked: bool,
    #[cfg(test)]
    #[config(env = "QVIEW_CONTAINER_LOGS", default = false)]
    pub container_logs: bool,
}

pub fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| {
        Config::builder()
            .env()
            .load()
            .expect("Failed to load one or more value configuration from the current environment")
    })
}
