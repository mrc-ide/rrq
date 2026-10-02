# Create worker configuration

Create a worker configuration, suitable to pass into
[rrq_worker_config_save](https://mrc-ide.github.io/rrq/reference/rrq_worker_config_save.md).
The results of this function should not be modified.

## Usage

``` r
rrq_worker_config(
  queue = NULL,
  verbose = TRUE,
  logdir = NULL,
  poll_queue = NULL,
  timeout_idle = Inf,
  poll_process = 1,
  timeout_process_die = 2,
  heartbeat_period = NULL,
  offload_threshold_size = Inf
)
```

## Arguments

- queue:

  Optional character vector of queues to listen on for tasks. There is a
  default queue which is always listened on (called 'default'). You can
  specify additional names here and tasks put onto these queues with
  [`rrq_task_create_expr()`](https://mrc-ide.github.io/rrq/reference/rrq_task_create_expr.md)
  (or other functions) will have *higher* priority than the default. You
  can explicitly list the "default" queue (e.g.,
  `queue = c("high", "default", "low")`) to set the position of the
  default queue.

- verbose:

  Logical, indicating if the worker should print logging output to the
  screen. Logging to screen has a small but measurable performance cost,
  and if you will not collect system logs from the worker then it is
  wasted time. Logging to the redis server is always enabled.

- logdir:

  Optional log directory to use for writing logs when queuing tasks in a
  separate process. If given, then logs will be saved to
  `<logdir>/<task_id>`. This directory should be writable by all workers
  and readable by the controller.

- poll_queue:

  Polling time for new tasks on the queue or messages. Longer values
  here will reduce the impact on the database but make workers less
  responsive to being killed with an interrupt (control-C or Escape).
  The default should be good for most uses, but shorter values are used
  for debugging. Importantly, longer times here do not increase the time
  taken for a worker to detect new tasks.

- timeout_idle:

  Optional timeout that sets the length of time after which the worker
  will exit if it has not processed a task. This is (roughly) equivalent
  to issuing a `TIMEOUT_SET` message after initialising the worker,
  except that it's guaranteed to be run by all workers.

- poll_process:

  Polling time indicating how long to wait for a background process to
  produce stdout or stderr. Only used for tasks queued with
  `separate_process` `TRUE`.

- timeout_process_die:

  Timeout indicating how long to wait wait for the background process to
  respond to SIGTERM, either as we stop a worker or cancel a task. Only
  used for tasks queued with `separate_process` `TRUE`. If your tasks
  may take several seconds to stop, you may want to increase this to
  ensure a clean exit.

- heartbeat_period:

  Optional period for the heartbeat. If non-NULL then a heartbeat
  process will be started (using
  [`rrq_heartbeat`](https://mrc-ide.github.io/rrq/reference/heartbeat.md))
  which can be used to build fault tolerant queues. See
  [`vignette("fault-tolerance")`](https://mrc-ide.github.io/rrq/articles/fault-tolerance.md)
  for details. If `NULL` (the default), then no heartbeat is configured.

- offload_threshold_size:

  The object size beyond which task results are offloaded to disk
  instead of being stored in Redis. See
  [`rrq_controller`](https://mrc-ide.github.io/rrq/reference/rrq_controller.md)
  for details. If `Inf` (the default), results are never offloaded.

## Value

A list of values with class `rrq_worker_config`; these should be
considered read-only, and contain only the validated input parameters.

## Examples

``` r
rrq::rrq_worker_config()
#> $queue
#> [1] "default"
#> 
#> $verbose
#> [1] TRUE
#> 
#> $logdir
#> NULL
#> 
#> $poll_queue
#> [1] 60
#> 
#> $timeout_idle
#> [1] Inf
#> 
#> $poll_process
#> [1] 1
#> 
#> $timeout_process_die
#> [1] 2
#> 
#> $heartbeat_period
#> NULL
#> 
#> $offload_threshold_size
#> [1] Inf
#> 
#> attr(,"class")
#> [1] "rrq_worker_config"
```
