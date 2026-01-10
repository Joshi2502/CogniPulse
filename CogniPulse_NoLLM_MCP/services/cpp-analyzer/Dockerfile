FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config \
    librdkafka-dev \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cmake -S . -B build && cmake --build build -j
CMD ["./build/cognipulse_analyzer"]
