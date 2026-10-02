# Returns the last (few) elements in the worker log, in a programmatically useful format (see Value).

Returns the last (few) elements in the worker log, in a programmatically
useful format (see Value).

## Usage

``` r
rrq_worker_log_tail(worker_ids = NULL, n = 1, controller = NULL)
```

## Arguments

- worker_ids:

  Optional vector of worker ids. If `NULL` then all active workers are
  used.

- n:

  Number of elements to select, the default being the single last entry.
  Use `Inf` or `0` to indicate that you want all log entries

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A [data.frame](https://rdrr.io/r/base/data.frame.html) with columns:

- `worker_id`: the worker id

- `child`: the process id, an integer, where logs come from a child
  process from a task queued with `separate_process = TRUE`

- `time`: the time from Redis when the event happened; see
  [redux::redis_time](https://richfitz.github.io/redux/reference/redis_time.html)
  to convert this to an R time

- `command`: the command sent from or to the worker

- `message`: the message corresponding to that command

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_worker_log_tail(n = 10, controller = obj)
}
```
