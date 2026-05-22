# Syscall Reference

Tachyon IPC lifecycle splits into three phases with very different syscall profiles. All observations are derived from
`strace` and the core source (`core/`). macOS differences are noted explicitly.

## Handshake (listen)

Called once per `tachyon_bus_listen()`. All syscalls are one-shot except poll.

| Syscall                  | Source                                                     | Reason                                                                                                                                                                   | Conditional |
|--------------------------|------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `memfd_create`           | `shm.cpp::SharedMemory::create`                            | Creates the anonymous SHM region (visible in `/proc/self/fd`).                                                                                                           | Linux only  |
| `ftruncate`              | `shm.cpp::SharedMemory::create`                            | Allocates the ring buffer capacity.                                                                                                                                      | Linux only  |
| `fchmod(0600)`           | `shm.cpp::SharedMemory::create`                            | Restricts memfd permissions (no-op in practice).                                                                                                                         | Linux only  |
| `fcntl(F_ADD_SEALS)`     | `shm.cpp::SharedMemory::create`                            | Prevents resize after format (`F_SEAL_SHRINK\|GROW\|SEAL`).                                                                                                              | Linux only  |
| `mmap(MAP_SHARED)`       | `shm.cpp::SharedMemory::create`                            | Maps the ring buffer.                                                                                                                                                    |             |
| `mmap(MAP_POPULATE)`     | `shm.cpp::SharedMemory::create`                            | Pre-faults all pages before handshake completes.                                                                                                                         | Linux only  |
| `madvise(MADV_DONTFORK)` | `shm.cpp::SharedMemory::create`                            | Prevents CoW inheritance in child processes.                                                                                                                             | Linux only  |
| `socket(AF_UNIX)`        | `transport_uds.cpp::uds_export_shm`                        | Opens the UDS endpoint.                                                                                                                                                  |             |
| `unlink`                 | `transport_uds.cpp::uds_export_shm`                        | Clears stale socket before `bind`. `ENOENT` on first run.                                                                                                                |             |
| `bind`                   | `transport_uds.cpp::uds_export_shm`                        | Binds to the socket path.                                                                                                                                                |             |
| `listen`                 | `transport_uds.cpp::uds_export_shm`                        | Marks the socket as passive. Backlog of 1.                                                                                                                               |             |
| `poll`                   | `transport_uds.cpp::uds_export_shm`                        | 100 ms timeout loop until `accept` succeeds.                                                                                                                             |             |
| `accept`                 | `transport_uds.cpp::uds_export_shm`                        | One-shot. Listening socket closed immediately after.                                                                                                                     |             |
| `sendmsg`                | `transport_uds.cpp::uds_export_shm` / `uds_export_shm_rpc` | Transfers handshake struct and 1 fd (SPSC) or 2 fds (RPC) via `SCM_RIGHTS`. `cmsg_len = CMSG_LEN(sizeof(int))` for SPSC; `cmsg_len = CMSG_LEN(2 * sizeof(int))` for RPC. |             |
| `close` x2               | `transport_uds.cpp::uds_export_shm`                        | Closes client socket and listening socket.                                                                                                                               |             |
| `unlink`                 | `transport_uds.cpp::uds_export_shm`                        | Removes the socket file. Path no longer exists after this.                                                                                                               |             |

On macOS, `memfd_create` / `ftruncate` / `fchmod` / `fcntl(F_ADD_SEALS)` are replaced by `shm_open` + immediate
`unlink` + `ftruncate`. The rest is identical.

---

### RPC handshake difference

`tachyon_rpc_listen` calls `uds_export_shm_rpc` instead of `uds_export_shm`. It creates two `SharedMemory` instances
(`shm_fwd`, `shm_rev`) and transfers both fds in a single `sendmsg`. All other syscalls in the handshake phase are
identical and called twice (once per arena). The hot path syscall profile is unchanged: `arena_fwd` and `arena_rev` are
independent SPSC rings. Each direction uses the same `futex(FUTEX_WAIT)` / `futex(FUTEX_WAKE)` pair as a standalone SPSC
bus.

## Handshake (connect)

Called once per `tachyon_bus_connect()`. Returns `TACHYON_ERR_NETWORK` immediately if the listener is not ready. Retry
is the caller's responsibility.

| Syscall                          | Source                              | Reason                                                                           | Conditional               |
|----------------------------------|-------------------------------------|----------------------------------------------------------------------------------|---------------------------|
| `socket(AF_UNIX)`                | `transport_uds.cpp::uds_import_shm` | Opens the UDS endpoint.                                                          |                           |
| `connect`                        | `transport_uds.cpp::uds_import_shm` | Fails with `ENOENT` if the listener is not ready.                                |                           |
| `close`                          | `transport_uds.cpp::uds_import_shm` | Closes the socket on connect failure.                                            | Failure path only         |
| `recvmsg`                        | `transport_uds.cpp::uds_import_shm` | Receives handshake struct and memfd fd via `SCM_RIGHTS`.                         |                           |
| `close`                          | `transport_uds.cpp::uds_import_shm` | Closes the UDS socket after `recvmsg`.                                           |                           |
| `mmap(MAP_SHARED\|MAP_POPULATE)` | `shm.cpp::SharedMemory::join`       | Maps the received memfd into the consumer address space and pre-faults all pages | `MAP_POPULATE` Linux only |
| `madvise(MADV_DONTFORK)`         | `shm.cpp::SharedMemory::join`       | Same as listen side.                                                             | Linux only                |

## Hot path (Linux)

- **Pure-spin mode** (`tachyon_bus_set_polling_mode(bus, 1)`): zero syscalls. Confirmed by strace.
- **Hybrid mode** (default): consumer spins up to `spin_threshold` iterations then sleeps via futex.

| Syscall             | Source                     | Reason                                                                 | Conditional                              |
|---------------------|----------------------------|------------------------------------------------------------------------|------------------------------------------|
| `futex(FUTEX_WAIT)` | `arena.cpp::platform_wait` | Consumer parks when the ring is empty. 200 ms timeout, then retry.     | Consumer only, under starvation          |
| `futex(FUTEX_WAKE)` | `arena.cpp::platform_wake` | Producer wakes the consumer on `flush_tx` if `consumer_sleeping == 1`. | Producer only, skipped in pure-spin mode |

## Hot path (macOS)

Same contract. `futex` is replaced by `__ulock_wait` / `__ulock_wake` (Apple primitives via `syscall()`). No seccomp BPF
on macOS.

## Configuration (post-handshake, one-shot)

| Syscall | Source                                     | Reason                                         | Conditional |
|---------|--------------------------------------------|------------------------------------------------|-------------|
| `mbind` | `tachyon_c.cpp::tachyon_bus_set_numa_node` | Migrates SHM pages to the requested NUMA node. | Linux only  |

## Teardown

Called once per `tachyon_bus_destroy()` when `ref_count` reaches zero.

| Syscall  | Source                           | Reason                            |
|----------|----------------------------------|-----------------------------------|
| `munmap` | `shm.cpp::SharedMemory::release` | Unmaps the SHM region.            |
| `close`  | `shm.cpp::SharedMemory::release` | Closes the memfd file descriptor. |

The kernel releases the anonymous memory when the last fd referencing it is closed. No filesystem cleanup is required.

## Binding notes

The syscall profile above applies to **C++/C and Rust only.** Every other binding runs with a managed runtime that emits
its own syscalls independently of Tachyon.

- **Go**: goroutine scheduler, GC, and netpoller emit `futex`, `clone`, `mmap`, `epoll_*` continuously. Any strict
  filter breaks the runtime.
- **Python**: GIL, GC, and signal handling emit arbitrary syscalls. The CPython extension is clean, the interpreter is
  not.
- **Java**: JVM emits `mmap`, `futex`, `clone`, `rt_sigaction` for thread and GC management. Panama FFM adds nothing,
  but the JVM baseline is wide.
- **Node.js**: libuv uses `epoll_wait`, `timerfd_*`, `eventfd`, and `io_uring` depending on version.

For polyglot deployments, apply containment at the process boundary via a supervisor (`systemd SystemCallFilter=`,
container seccomp profile) rather than from within the process. Pre-built profiles for C++/C only deployments are in
`contrib/seccomp/`.

---

## Star Bus syscall profile

### Handshake

`tachyon_star_create()` performs no syscalls of its own. It is a pure user-space operation that stores the N connector
handles and calibrates the TSC polling budget via `rdtsc()` (`RDTSC` on x86) (user-space instruction, no syscall). All
handshake syscalls occur during the preceding `tachyon_bus_connect()` calls, one full connect sequence per spoke,
identical to the SPSC connect table above.

### Hot path

The star hot path adds zero new syscall types beyond the single-bus SPSC profile.

| Syscall             | Condition                             | Notes                                            |
|---------------------|---------------------------------------|--------------------------------------------------|
| `futex(FUTEX_WAIT)` | Per-spoke, consumer only, hybrid mode | One futex domain per spoke; they are independent |
| `futex(FUTEX_WAKE)` | Per-spoke, producer only, hybrid mode | Skipped in pure-spin mode                        |

`tachyon_star_poll()` iterates over all N connector arenas in user space. `RDTSC` is used to check the budget deadline
on each iteration; it is a user-space instruction and does not enter the kernel. The poll exits via `cpu_relax()`
(`PAUSE` on x86) when all spokes are empty within the budget.

`tachyon_star_commit()` advances the consumer tail of each spoke that had messages, using atomic stores.

In pure-spin mode (all N spokes set via `tachyon_bus_set_polling_mode(bus, 1)` before `tachyon_star_create()`), the hot
path contains zero-syscall.

### NUMA binding at create

If `node_ids` is non-null, `tachyon_star_create()` calls`tachyon_bus_set_numa_node()` for each spoke with a non-negative
entry. Each call emits one `mbind` syscall, identical to the single-bus NUMA section above. This is a one-shot setup
cost, not a recurring hot-path cost.

| Syscall | Source                                                | Reason                                                     | Conditional                                    |
|---------|-------------------------------------------------------|------------------------------------------------------------|------------------------------------------------|
| `mbind` | `tachyon_star_create` via `tachyon_bus_set_numa_node` | Migrates each spoke's SHM pages to the requested NUMA node | Once per spoke with non-negative `node_ids[i]` |

### Teardown

`tachyon_star_destroy()` is a user-space operation. It decrements the ref-count on each stored bus handle; when a
ref-count reaches zero, `tachyon_bus_destroy()` is called for that handle, emitting `munmap` + `close` as in the
single-bus teardown table above.

### Binding notes

The zero-syscall hot path applies to C/C++ and Rust only. Language binding overheads are unchanged from the SPSC case.
Go, Python, Java, and Node.js runtimes emit their own syscalls independently; see the binding notes in the SPSC hot-path
section.
