# Task Queue with Valkey and GLIDE

A simple yet powerful task queue implementation using Valkey streams and the GLIDE client library. Available in both Python and Node.js.

## What's Inside?

- Stream-based task queue with batch processing
- Producer-Consumer pattern implementation
- Threshold-based batch processing
- Both Python and Node.js implementations
- Dockerized Valkey cluster setup

## Quick Start

1. Start the Valkey cluster:
```bash
docker-compose up -d
```

2. Choose your implementation:

Python:
```bash
cd python
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

Node.js:
```bash
cd node
npm install
npm run dev
```

## About the Implementation

- Uses Valkey GLIDE client library
- Implements stream commands (XADD, XREADGROUP, XACK)
- Batch processing with configurable threshold
- Consumer group pattern for reliable message processing

## About Valkey GLIDE

[Valkey GLIDE](https://github.com/valkey-io/valkey-glide) is a multi-language client library supporting Python, Node.js, Java, and Go (with Ruby and C++ under development). It's designed for:

- High reliability and performance
- Consistent features across languages
- Best practices from AWS's client team experience
- Compatible with Valkey 7.2+ and Redis OSS 6.2+

## Architecture

```
Producer → Valkey Stream → Consumer Group → Batch Processing
```

The producer adds tasks to a Valkey stream, while consumers read and process tasks in batches when either:
- The stream reaches the threshold (5 tasks)
- The producer signals completion
