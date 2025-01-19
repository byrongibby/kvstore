# gRPC Key Value store

## Rewrite

This is my attempt at a Clojure rewrite of [https://raw.githubusercontent.com/carl-mastrangelo/kvstore](https://github.com/carl-mastrangelo/kvstore), the original blog that goes with the code can be found on the [gRPC](https://grpc.io/blog/optimizing-grpc-part-1/) website.

## How to run

This repo is an example key value store written in gRPC.  The client and server run in the same binary.  You can run the demo by installing Clojure and running:

```
clj -M -m io.grpc.examples.kv-runner
```
