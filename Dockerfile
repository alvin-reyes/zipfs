FROM kassany/bookworm-ziglang:0.15.1 AS builder
WORKDIR /app
COPY . .
RUN zig build -Doptimize=ReleaseFast

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/zig-out/bin/zipfs /usr/local/bin/zipfs
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh && mkdir -p /data/zipfs
ENV IPFS_PATH=/data/zipfs
EXPOSE 4001 8080
CMD ["/usr/local/bin/entrypoint.sh"]
