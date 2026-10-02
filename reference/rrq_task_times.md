# Fetch task times

Fetch times for tasks at points in their life cycle. For each task
returns the time of submission, starting and completion (not necessarily
successfully; this includes errors and interruptions). If a task has not
reached a point yet (e.g., submitted but not run, or running but not
finished) the time will be `NA`). Times are returned in unix timestamp
format in UTC; you can use
[redux::redis_time_to_r](https://richfitz.github.io/redux/reference/redis_time.html)
to convert them to a POSIXt object.

## Usage

``` r
rrq_task_times(task_ids, follow = NULL, controller = NULL)
```

## Arguments

- task_ids:

  A vector of task ids

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

A matrix of times, with row names corresponding to task ids. We may
change this to a data.frame at some point in the future.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

t <- rrq_task_create_expr(Sys.sleep(3), controller = obj)
rrq_task_times(t, controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_times(t, controller = obj)
}
```
