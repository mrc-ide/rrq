# Spawn a worker

Spawn a worker in the background

## Usage

``` r
rrq_worker_spawn(
  n = 1,
  logdir = NULL,
  timeout = 600,
  name_config = "localhost",
  worker_id_base = NULL,
  time_poll = 0.2,
  progress = NULL,
  controller = NULL,
  offload_path = NULL
)
```

## Arguments

- n:

  Number of workers to spawn

- logdir:

  Path of a log directory to write the worker process log to,
  interpreted relative to the current working directory

- timeout:

  Time to wait for workers to appear. If 0 then we don't wait for
  workers to appear (you can run the `wait_alive` method of the returned
  object to run this test manually)

- name_config:

  Name of the configuration to use. By default the `"localhost"`
  configuration is used

- worker_id_base:

  Optional base to construct the worker ids from. If omitted a random
  base will be used. Actual ids will be created but appending integers
  to this base.

- time_poll:

  Polling period (in seconds) while waiting for workers to come up.

- progress:

  Show a progress bar while waiting for workers (when `timeout` is at
  least 0)

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

- offload_path:

  The path to create an offload store at. See
  [rrq_controller](https://mrc-ide.github.io/rrq/reference/rrq_controller.md)
  for details.

## Value

An `rrq_worker_manager` object with fields:

- `id`: the ids of the spawned workers

- `wait_alive`: a method to wait for workers to come alive

- `stop`: a method to stop workers

- `kill`: a method to kill workers abruptly by sending a signal

- `is_alive`: a method that checks if a worker is currently alive

- `logs`: a method that returns logs for a single worker

All the methods accept a vector of worker names, or integers, except
`logs` which requires a single worker id (as a string or integer). For
all methods except `logs`, the default of `NULL` means "all managed
workers".

## Details

Spawning multiple workers. If `n` is greater than one, multiple workers
will be spawned. This happens in parallel so it does not take n times
longer than spawning a single worker.

Beware that signals like Ctrl-C passed to *this* R instance can still
propagate to the child processes and can result in them dying
unexpectedly. It is probably safer to start processes in a completely
separate session.
