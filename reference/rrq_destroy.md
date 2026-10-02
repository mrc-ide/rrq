# Destroy queue

Entirely destroy a queue, by deleting all keys associated with it from
the Redis database. This is a very destructive action and cannot be
undone.

## Usage

``` r
rrq_destroy(
  delete = TRUE,
  worker_stop_type = "message",
  timeout_worker_stop = 0,
  controller = NULL
)
```

## Arguments

- delete:

  Either `TRUE` (the default) indicating that the keys should be
  immediately deleted. Alternatively, provide an integer value and the
  keys will instead be marked for future deletion by "expiring" after
  this many seconds, using Redis' `EXPIRE` command.

- worker_stop_type:

  Passed to
  [`rrq_worker_stop()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_stop.md);
  Can be one of "message", "kill" or "kill_local". The "kill" method
  requires that the workers are using a heartbeat, and "kill_local"
  requires that the workers are on the same machine as the controller.
  However, these may be faster to stop workers than "message", which
  will wait until any task is finished.

- timeout_worker_stop:

  A timeout to pass to the worker to respond the request to stop. See
  `worker_stop`'s `timeout` argument for details.

- controller:

  The controller to destroy
