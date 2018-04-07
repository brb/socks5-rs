# socks5-rs


*This project has been created for learning purpose (Rust), and it should
never be used in production.*

SOCKS5 (RFC 1928) implementation in Rust.

The project consists of the following parts:

* `src/bin`: SOCKS5 protocol implementation based on `tcph`.
* `workers`: A thread pool.
* `tcph`: A generic multi-threaded TCP handler for implementing custom
  TCP protocols based on the exported `FSM` trait.

## Building

```
$ cargo build
```

Tested with `rustc 1.25.0 (84203cac6 2018-03-25)`.
