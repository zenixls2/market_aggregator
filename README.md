Market Aggregator
-----------------

## Requirements

* rust nightly version should not be older than 2023's build

## Quick start

Open two terminals:

1.

```bash
# folder: project root
cargo run --bin server -- -c config/config.yaml
```

2.

```bash
# folder; project root
cargo run --bin client -- -c config/config.yaml
```

And you should be able to see 2. starts to output messages from grpc server (which is from 1.)

## Features
- Merge market data from two exchanges. Have the flexibility to extend to more.
- Basic log functionality
- Include both the grpc client and server implementation

## Development

1. Before making pr, remember to run `cargo fmt`, `cargo clippy`, and passed the `cargo test`.
2. Currently there's no github action for building and testing the sources.
