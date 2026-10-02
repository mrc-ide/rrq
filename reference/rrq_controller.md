# Create rrq controller

Create a new controller. This is the new interface that will replace
rrq_controller soon, at which point it will rename back to
`rrq_controller`.

## Usage

``` r
rrq_controller(
  queue_id,
  con = redux::hiredis(),
  timeout_task_wait = NULL,
  follow = NULL,
  check_version = TRUE,
  offload_path = NULL,
  offload_threshold_size = Inf
)
```

## Arguments

- queue_id:

  An identifier for the queue. This will prefix all keys in redis, so a
  prefix might be useful here depending on your use case (e.g.
  `rrq:<user>:<id>`)

- con:

  A redis connection. The default tries to create a redis connection
  using default ports, or environment variables set as in
  [`redux::hiredis()`](https://richfitz.github.io/redux/reference/hiredis.html)

- timeout_task_wait:

  An optional default timeout to use when waiting for tasks with
  [rrq_task_wait](https://mrc-ide.github.io/rrq/reference/rrq_task_wait.md).
  If not given, then we fall back on the global option
  `rrq.timeout_task_wait`, and if that is not set, we wait forever
  (i.e., `timeout_task_wait = Inf`).

- follow:

  An optional default logical to use for tasks that may (or may not) be
  retried. If not given we fall back on the global option `rrq.follow`,
  and if that is not set then `TRUE` (i.e., we do follow). The value
  `follow = TRUE` is potentially slower than `follow = FALSE` for some
  operations because we need to dereference every task id. If you never
  use
  [rrq_task_retry](https://mrc-ide.github.io/rrq/reference/rrq_task_retry.md)
  then this dereference never has an effect and we can skip it. See
  [`vignette("fault-tolerance")`](https://mrc-ide.github.io/rrq/articles/fault-tolerance.md)
  for more information.

- check_version:

  Logical, indicating if we should check the schema version. You can
  pass `FALSE` here to continue even where the schema version is
  incompatible, though any subsequent actions may lead to corruption.

- offload_path:

  The path to create an offload store at (passed to
  [`object_store_offload_disk`](https://mrc-ide.github.io/rrq/reference/object_store_offload_disk.md)).
  The directory will be created if it does not exist. If not given (or
  `NULL`) but the queue was configured with a finite
  `offload_threshold_size`, trying to save large objects will throw an
  error.

- offload_threshold_size:

  The maximum object size, in bytes, before being moved to the offload
  store. If given, then larger data will be saved in `offload_path`
  (using
  [`object_store_offload_disk`](https://mrc-ide.github.io/rrq/reference/object_store_offload_disk.md))

## Value

An `rrq_controller` object, which is opaque.

## Task lifecycle

- A task is queued with `$enqueue()`, at which point it becomes
  `PENDING`

- Once a worker selects the task to run, it becomes `RUNNING`

- If the task completes successfully without error it becomes `COMPLETE`

- If the task throws an error, it becomes `ERROR`

- If the task was cancelled (e.g., via `$task_cancel()`) it becomes
  `CANCELLED`

- If the task is killed by an external process, crashes or the worker
  dies (and is running a heartbeat) then the task becomes `DIED`.

- The status of an unknown task is `MISSING`

- Tasks in any terminal state (except `IMPOSSIBLE`) may be retried with
  `task_retry` at which point they become `MOVED`, see
  [`vignette("fault-tolerance")`](https://mrc-ide.github.io/rrq/articles/fault-tolerance.md)
  for details

## Worker lifecycle

- A worker appears and is `IDLE`

- When running a task it is `BUSY`

- If it receives a `PAUSE` message it becomes `PAUSED` until it receives
  a `RESUME` message

- If it exits cleanly (e.g., via a `STOP` message or a timeout) it
  becomes `EXITED`

- If it crashes and was running a heartbeat, it becomes `LOST`

## Messages

Most of the time workers process tasks, but you can also send them
"messages". Messages take priority over tasks, so if a worker becomes
idle (by coming online or by finishing a task) it will consume all
available messages before starting on a new task, even if both are
available.

Each message has a "command" and may have "arguments" to that command.
The supported messages are:

- `PING` (no args): "ping" the worker, if alive it will respond with
  "PONG"

- `ECHO` (accepts an argument of a string): Print a string to the
  terminal and log of the worker. Will respond with `OK` once the
  message has been printed.

- `EVAL` (accepts a string or a quoted expression): Evaluate an
  arbitrary R expression on the worker. Responds with the value of this
  expression.

- `STOP` (accepts a string to print as the worker exits, defaults to
  "BYE"): Tells the worker to stop.

- `INFO` (no args): Returns information about the worker (versions of
  packages, hostname, pid, etc).

- `PAUSE` (no args): Tells the worker to stop accepting tasks (until it
  receives a `RESUME` message). Messages are processed as normal.

- `RESUME` (no args): Tells a paused worker to resume accepting tasks.

- `REFRESH` (no args): Tells the worker to rebuild their environment
  with the `create` method.

- `TIMEOUT_SET` (accepts a number, representing seconds): Updates the
  worker timeout - the length of time after which it will exit if it has
  not processed a task.

- `TIMEOUT_GET` (no args): Tells the worker to respond with its current
  timeout.

## Storage

Every time a task is saved, or a task is completed, results are saved
into the Redis database. Because Redis is an in-memory database, it's
not a great idea to save very large objects into it (if you ran 100 jobs
in parallel and each saved a 2GB object you'd likely take down your
redis server). In addition, `redux` does not support directly saving
objects larger than `2^31 - 1` bytes into Redis. So, for some use cases
we need to consider where to store larger objects.

The strategy here is to "offload" the larger objects - bigger than some
user-given size - onto some other storage system. Currently the only
alternative supported is a disk store
([`object_store_offload_disk`](https://mrc-ide.github.io/rrq/reference/object_store_offload_disk.md))
but we hope to expand this later. So if your task returns a 3GB object
then we will spill that to disk rather than failing to save that into
Redis.

The storage directory for offloading must be shared amoung all users of
the queue. Depending on the usecase, this could be a directory on the
local filesystems or, if using a queue across machines, it can be a
network file system mounted on all machines.

How big is an object? We serialise the object
([`redux::object_to_bin`](https://richfitz.github.io/redux/reference/object_to_string.html)
just wraps [`serialize`](https://rdrr.io/r/base/serialize.html)) which
creates a vector of bytes and that is saved into the database. To get an
idea of how large things are you can do:
`length(redux::object_to_bin(your_object))`. At the time this
documentation was written, `mtcars` was `3807` bytes, and a million
random numbers was `8,000,031` bytes. It's unlikely that a
`offload_threshold_size` of less than 1MB will be sensible.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")

# Create a new controller; the id will be specific to your
# application.  Here, we use 'rrq:example'
obj <- rrq_controller("rrq:example")

# Create a task for this controller to work on:
t <- rrq_task_create_expr(runif(10), controller = obj)

# Wait for the task to complete
rrq_task_wait(t, controller = obj)

# Fetch the task's result
rrq_task_result(t, controller = obj)
}
```
