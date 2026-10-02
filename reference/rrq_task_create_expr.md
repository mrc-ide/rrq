# Create a task based on an expression

Create a task based on an expression. The expression passed as `expr`
will typically be a function call (e.g., `f(x)`). We will analyse the
expression and find all variables that you reference (in the case of
`f(x)` this is `x`) and combine this with the function name to run on
the worker. If `x` cannot be found in your calling environment we will
error.

## Usage

``` r
rrq_task_create_expr(
  expr,
  queue = NULL,
  separate_process = FALSE,
  timeout_task_run = NULL,
  depends_on = NULL,
  controller = NULL
)
```

## Arguments

- expr:

  The expression, does not need quoting. See Details.

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

## Details

Alternatively you may provide a multiline statement by using
[`{}`](https://rdrr.io/r/base/Paren.html) to surround multiple lines,
such as:

    task_create_expr({
      x <- runif(1)
      f(x)
    }, ...)

in this case, we apply a simple heuristic to work out that `x` is
locally assigned and should not be saved with the expression.

## See also

[rrq_task_create_call](https://mrc-ide.github.io/rrq/reference/rrq_task_create_call.md)
for creating a task from a function and arguments to the function, and
[rrq_task_create_bulk_expr](https://mrc-ide.github.io/rrq/reference/rrq_task_create_bulk_expr.md)
for creating many tasks from a call and a data.frame

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

# Simple use of the function to create a task based on a function call
t <- rrq_task_create_expr(sqrt(2), controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)

# The expression can contain calls to other variables, and these
# will be included in the call:
a <- 3
t <- rrq_task_create_expr(sqrt(a), controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)

# You can pass in an expression _as_ a symbol too:
expr <- quote(sqrt(4))
t <- rrq_task_create_expr(expr, controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)

# If you queue tasks into separate processes you can use a timeout
# to kill the task if it takes too long:
t <- rrq_task_create_expr(Sys.sleep(3),
                          separate_process = TRUE,
                          timeout_task_run = 1,
                          controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)
}
```
