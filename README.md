# Parallel Firewall (POSIX threads)

A multithreaded packet firewall in C. A **producer thread** reads packets into a thread-safe **circular buffer**. Multiple **consumer threads** take them out, run them through the filter (PASS/DROP by source address range), hash them and write the decisions to a log, **ordered by timestamp**.

The implementation passes all **20/20** tests of the course test suite. The tests cover 10 to 20,000 packets, with 1, 2 and 4 consumer threads, both with and without ordered output.

## Architecture

```
               ┌──────────────────────────┐
 input file ─▶ │ producer: publish_data() │
               └────────────┬─────────────┘
                            │ ring_buffer_enqueue
                   ┌────────▼────────┐
                   │  ring buffer    │  mutex + 2 condition variables
                   └────────┬────────┘
             ring_buffer_dequeue (blocking)
          ┌─────────────┼─────────────┐
     consumer 1    consumer 2  …  consumer N   filter + hash
          └─────────────┼─────────────┘
                 timestamp-ordered log
```

## Implementation

### Ring buffer: `src/ring_buffer.c`
- A fixed-capacity byte buffer with `read_pos`, `write_pos` and `len`, all wrapping around modulo the capacity.
- One **mutex** guards the state. Two **condition variables** handle back-pressure:
  - `enqueue` waits while the buffer is full;
  - `dequeue` waits while there is not enough data.
- **Graceful shutdown:** `ring_buffer_stop()` sets a flag and broadcasts to every waiting consumer. `dequeue` returns `-1` once the buffer is both stopped and empty, which tells the consumer threads to exit.

### Consumers: `src/consumer.c`
- Each thread dequeues a packet, calls `process_packet()` (filter) and `packet_hash()`, and records `(action, hash, timestamp)` in a shared array protected by a mutex.
- **Ordered output:** packets reach the consumers out of order. The shared array therefore works as a reorder window: when it fills up to 100 entries, it is sorted by timestamp, the oldest 50 are written to the log and the rest are kept. Whatever remains is flushed in order when the threads finish.

### Main: `src/firewall.c`
Initializes the ring buffer, starts 1 to 32 consumers, runs the producer on the main thread and joins all consumers.

`src/serial.c` is the single-threaded reference version.

## Build & run

```bash
cd src
make                                   # builds ./firewall and ./serial
./firewall ../tests/in/test_10_000.in out.log 4
```

To run the test suite:

```bash
cd tests
make check
```

## Repository layout

```
src/ring_buffer.c    thread-safe circular buffer
src/consumer.c       consumer threads + timestamp ordering
src/producer.c       producer (reads packets from file)
src/firewall.c       entry point
src/packet.c         packet filter + hash (provided)
src/serial.c         serial reference implementation (provided)
utils/               logging utilities
tests/               checker, input packets, reference outputs
```

## Credits

Developed as an assignment for the *Operating Systems* course at the Faculty of Automatic Control and Computers, Politehnica University of Bucharest (2024). The course team provided the skeleton, the packet code, the serial version, `utils/` and the tests. The ring buffer, the consumer threads and the thread management are my own work.
