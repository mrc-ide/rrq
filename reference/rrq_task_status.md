# Fetch task statuses

Return a character vector of task statuses. The name of each element
corresponds to a task id, and the value will be one of the possible
statuses ("PENDING", "COMPLETE", etc).

## Usage

``` r
rrq_task_status(task_ids, named = FALSE, follow = NULL, controller = NULL)
```

## Arguments

- task_ids:

  Optional character vector of task ids for which you would like
  statuses.

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

A character vector the same length as `task_ids`

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

ts <- rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
rrq_task_status(ts, controller = obj)
rrq_task_wait(ts, controller = obj)
rrq_task_status(ts, controller = obj)
}
```
