# Timer

Simple implementation of a Timer in async Rust.

# Example

```rust
fn main() {
    let timer = Timer::new(
        callback,
        notify_shutdown.subscribe().into(),
    );

    timer.schedule(Utc::now().naive_utc());

    timer.await;
}

fn callback() -> Option<NaiveDateTime> {
    println!("Hello, World!");
    None
}
```