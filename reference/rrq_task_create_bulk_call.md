# Create bulk tasks from a call

Create a bulk set of tasks based on applying a function over a vector or
[data.frame](https://rdrr.io/r/base/data.frame.html). This is the bulk
equivalent of
[rrq_task_create_call](https://mrc-ide.github.io/rrq/reference/rrq_task_create_call.md),
in the same way that
[rrq_task_create_bulk_expr](https://mrc-ide.github.io/rrq/reference/rrq_task_create_bulk_expr.md)
is a bulk version of
[rrq_task_create_expr](https://mrc-ide.github.io/rrq/reference/rrq_task_create_expr.md).

## Usage

``` r
rrq_task_create_bulk_call(
  fn,
  data,
  args = NULL,
  queue = NULL,
  separate_process = FALSE,
  timeout_task_run = NULL,
  depends_on = NULL,
  controller = NULL
)
```

## Arguments

- fn:

  The function to call

- data:

  The data to apply the function over. This can be a vector or list, in
  which case we act like `lapply` and apply `fn` to each element in
  turn. Alternatively, this can be a
  [data.frame](https://rdrr.io/r/base/data.frame.html), in which case
  each row is taken as a set of arguments to `fn`. Note that if `data`
  is a `data.frame` then all arguments to `fn` are named.

- args:

  Additional arguments to `fn`, shared across all calls. These must be
  named. If you are using a `data.frame` for `data`, you'd probably be
  better off adding additional columns that don't vary across rows, but
  the end result is the same.

- queue:

  The queue to add the task to; if not specified the "default" queue
  (which all workers listen to) will be used. If you have configured
  workers to listen to more than one queue you can specify that here. Be
  warned that if you push jobs onto a queue with no worker, it will
  queue forever.

- separate_process:

  Logical, indicating if the task should be run in a separate process on
  the worker. If `TRUE`, then the worker runs the task in a separate
  process using the `callr` package. This means that the worker
  environment is completely clean, subsequent runs are not affected by
  preceding ones. The downside of this approach is a considerable
  overhead in starting the external process and transferring data back.

- timeout_task_run:

  Optionally, a maximum allowed running time, in seconds. This parameter
  only has an effect if `separate_process` is `TRUE`. If given, then if
  the task takes longer than this time it will be stopped and the task
  status set to `TIMEOUT`.

- depends_on:

  Vector or list of IDs of tasks which must have completed before this
  job can be run. Once all dependent tasks have been successfully run,
  this task will get added to the queue. If the dependent task fails
  then this task will be removed from the queue.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A vector of task identifiers; this will have the length as `data` has
rows if it is a `data.frame`, otherwise it has the same length as `data`

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

d <- data.frame(n = 1:10, lambda = rgamma(10, 5))
ts <- rrq_task_create_bulk_call(rpois, d, controller = obj)
rrq_task_wait(ts, controller = obj)
rrq_task_results(ts, controller = obj)
}
```
