# Create bulk tasks from an expression

Create a bulk set of tasks. Variables in `data` take precedence over
variables in the environment in which `expr` was created. There is no
"pronoun" support yet (see rlang docs). Use `!!` to pull a variable from
the environment if you need to, but be careful not to inject something
really large (e.g., any vector really) or you'll end up with a revolting
expression and poor backtraces.

## Usage

``` r
rrq_task_create_bulk_expr(
  expr,
  data,
  queue = NULL,
  separate_process = FALSE,
  timeout_task_run = NULL,
  depends_on = NULL,
  controller = NULL
)
```

## Arguments

- expr:

  An expression, as for
  [rrq_task_create_expr](https://mrc-ide.github.io/rrq/reference/rrq_task_create_expr.md)

- data:

  Data that you wish to inject *row-wise* into the expression

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

A character vector with task identifiers; this will have a length equal
to the number of row in `data`

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

# Create 10 tasks:
ts <- rrq_task_create_bulk_expr(sqrt(x), data.frame(x = 1:10),
                                controller = obj)
rrq_task_wait(ts, controller = obj)
rrq_task_results(ts, controller = obj)

# Note that there is no automatic simplification when fetching
# results, you might use unlist or vapply to turn this into a
# numeric vector rather than a list

# The data.frame substituted in may have multiple columns
# representing multiple variables to substitute into the
# expression
d <- expand.grid(a = 1:4, b = 1:4)
ts <- rrq_task_create_bulk_expr(a * b, d, controller = obj)
rrq_task_wait(ts, controller = obj)
rrq_task_results(ts, controller = obj)
}
```
