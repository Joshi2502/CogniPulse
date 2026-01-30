FROM ubuntu:22.04
WORKDIR /work

RUN apt-get update && apt-get install -y \
  build-essential cmake pkg-config \
  librdkafka-dev nlohmann-json3-dev \
  && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cmake -S . -B build && cmake --build build -j
CMD ["./build/analyzer"]
