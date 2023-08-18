# Timer

Simple implementation of a Timer in async Rust.

# Example

```rust
let task = || {
    eprintln!("task was executed");
    None
};

let timer = Timer::new(task).with_graceful_shutdown(signal::ctrl_c());

timer.await;
```