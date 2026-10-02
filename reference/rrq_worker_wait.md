# Wait for workers

Wait for workers to appear.

## Usage

``` r
rrq_worker_wait(
  worker_ids,
  timeout = Inf,
  time_poll = 0.2,
  is_dead = NULL,
  fetch_logs = NULL,
  progress = NULL,
  controller = NULL
)
```

## Arguments

- worker_ids:

  A vector of worker ids to wait for

- timeout:

  Timeout in seconds; default is to wait forever

- time_poll:

  Poll interval, in seconds. Must be an integer

- is_dead:

  Optionally, a function which should accept zero arguments and return a
  logical vector along `worker_ids` which is `TRUE` if the worker has
  failed irrecoverably. This would be the case for example if the
  process has died.

- fetch_logs:

  Optionally, a function which should accept a single worker identifier
  and return a character vector of logs (or `NULL` if no logs are
  available)

- progress:

  Optional logical indicating if a progress bar should be displayed. If
  `NULL` we fall back on the value of the global option `rrq.progress`,
  and if that is unset display a progress bar if in an interactive
  session.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).
