# Fetch single task result

Get the result for a single task (see
[rrq_task_results](https://mrc-ide.github.io/rrq/reference/rrq_task_results.md)
for a method for efficiently getting multiple results at once). Returns
the value of running the task if it is complete, and an error otherwise.

## Usage

``` r
rrq_task_result(task_id, error = FALSE, follow = NULL, controller = NULL)
```

## Arguments

- task_id:

  The single id for which the result is wanted.

- error:

  Logical, indicating if we should throw an error if a task was not
  successful. By default (`error = FALSE`), in the case of the task
  result returning an error we return an object of class
  `rrq_task_error`, which contains information about the error. Passing
  `error = TRUE` calls [`stop()`](https://rdrr.io/r/base/stop.html) on
  this error if it is returned.

- follow:

  Optional logical, indicating if we should follow any redirects set up
  by doing
  [rrq_task_retry](https://mrc-ide.github.io/rrq/reference/rrq_task_retry.md).
  If not given, falls back on the value passed into the controller, the
  global option `rrq.follow`, and finally `TRUE`. Set to `FALSE` if you
  want to return information about the original task, even if it has
  been subsequently retried.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

The result of your task. This may be an error (an object with class
`rrq_task_error`) if your task has failed.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

# Create a task, wait for it to finish and fetch its result
t <- rrq_task_create_expr(runif(1), controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)

# Tasks that fail do not fail on result, but instead return an
# object with the class "rrq_task_error"
t <- rrq_task_create_expr(readRDS("somefile.rds"), controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)
}
```
