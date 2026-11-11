# The desktop client is not in this image. Build on the machine that will run it,
# including a Raspberry Pi, so the binary matches that CPU.
FROM rust:bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build -p ryme-server --release --locked

FROM debian:bookworm-slim
RUN useradd \
      --uid 1542 \
      --user-group \
      --home-dir /var/lib/ryme \
      --create-home \
      --shell /usr/sbin/nologin \
      ryme
COPY --from=build /src/target/release/ryme-server /usr/local/bin/ryme-server
COPY docker/entrypoint.sh /usr/local/bin/ryme-entrypoint
RUN chmod 0755 /usr/local/bin/ryme-server /usr/local/bin/ryme-entrypoint
ENV RYME_DATA=/var/lib/ryme \
    RYME_BIND=0.0.0.0:1542
EXPOSE 1542/udp
VOLUME ["/var/lib/ryme"]
ENTRYPOINT ["/usr/local/bin/ryme-entrypoint"]
