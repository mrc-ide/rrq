# Get the results of a group of tasks, returning them as a list. See [rrq_task_result](https://mrc-ide.github.io/rrq/reference/rrq_task_result.md) for getting the result of a single task.

Get the results of a group of tasks, returning them as a list. See
[rrq_task_result](https://mrc-ide.github.io/rrq/reference/rrq_task_result.md)
for getting the result of a single task.

## Usage

``` r
rrq_task_results(
  task_ids,
  error = FALSE,
  named = FALSE,
  follow = NULL,
  controller = NULL
)
```

## Arguments

- task_ids:

  A vector of task ids for which the task result is wanted.

- error:

  Logical, indicating if we should throw an error if the task was not
  successful. See
  [`rrq_task_result()`](https://mrc-ide.github.io/rrq/reference/rrq_task_result.md)
  for details.

- named:

  Logical, indicating if the return value should be named with the task
  ids; as these are quite long this can make the value a little awkward
  to work with.

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

A list, one entry per result. This function errors if any task is not
available. If `named = TRUE`, then this list is named with the
`task_ids`.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

ts <- rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
rrq_task_wait(ts, controller = obj)
rrq_task_results(ts, controller = obj)

# For a single task, rrq_task_result and rrq_task_results differ
# in the return type; rrq_task_results always returns a list:
t <- ts[[1]]
rrq_task_result(t, controller = obj)
rrq_task_results(t, controller = obj)
}
```
