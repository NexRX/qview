# Testing

Run the following command to start the test suite:

```sh
cargo test
# Or with logs enabled (trace | debug | info | warn | error)
RUST_LOG=qview=trace cargo test -- --nocapture
```
