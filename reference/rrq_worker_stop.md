# Stop workers

Stop workers, causing them to exit. Workers can be stopped in a few
different ways (see Details), but after executing this function, assume
that any worker targeted will no longer be available to work on tasks.

## Usage

``` r
rrq_worker_stop(
  worker_ids = NULL,
  type = "message",
  timeout = 0,
  time_poll = 0.1,
  progress = NULL,
  controller = NULL
)
```

## Arguments

- worker_ids:

  Optional vector of worker ids. If `NULL` then all active workers will
  be stopped.

- type:

  The strategy used to stop the workers. Can be `message`, `kill` or
  `kill_local` (see Details).

- timeout:

  Optional timeout; if greater than zero then we poll for a response
  from the worker for this many seconds until they acknowledge the
  message and stop (only has an effect if `type` is `message`). If a
  timeout of greater than zero is given, then for a `message`-based stop
  we wait up to this many seconds for the worker to exit. That means
  that we might wait up to `2 * timeout` seconds for this function to
  return.

- time_poll:

  If `type` is `message` and `timeout` is greater than zero, this is the
  polling interval used between redis calls. Increasing this reduces
  network load but decreases the ability to interrupt the process.

- progress:

  Optional logical indicating if a progress bar should be displayed. If
  `NULL` we fall back on the value of the global option `rrq.progress`,
  and if that is unset display a progress bar if in an interactive
  session.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

The names of the stopped workers, invisibly.

## Details

The `type` parameter indicates the strategy used to stop workers, and
interacts with other parameters. The strategies used by the different
values are:

- `message`, in which case a `STOP` message will be sent to the worker,
  which they will receive after finishing any currently running task (if
  `RUNNING`; `IDLE` workers will stop immediately).

- `kill`, in which case a kill signal will be sent via the heartbeat (if
  the worker is using one). This will kill the worker even if is
  currently working on a task, eventually leaving that task with a
  status of `DIED`.

- `kill_local`, in which case a kill signal is sent using operating
  system signals, which requires that the worker is on the same machine
  as the controller.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
w <- rrq_worker_spawn(controller = obj)
rrq_worker_list(controller = obj)
rrq_worker_stop(w$id, timeout = 10, controller = obj)
rrq_worker_list(controller = obj)
}
```
