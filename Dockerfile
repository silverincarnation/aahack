# Shopping Sessions: native Rust pipeline.
# Build stage: compile one binary. The scorers are linux/amd64 Intel Xeon (Cascade Lake or
# newer), so AVX2 and AES are safe to target.
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+aes"
RUN cargo build --release --locked && cp target/release/ss /ss

# Run stage: just the binary. At run time the root filesystem is read-only, the user is
# 1000:1000, there is no network; the binary writes only to $OUTPUT_DIR.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /ss /app/ss
CMD ["/app/ss"]
