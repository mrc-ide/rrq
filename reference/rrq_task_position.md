# Find task position in queue

Find the position of one or more tasks in the queue.

## Usage

``` r
rrq_task_position(
  task_ids,
  missing = 0L,
  queue = NULL,
  follow = NULL,
  controller = NULL
)
```

## Arguments

- task_ids:

  Character vector of tasks to find the position for.

- missing:

  Value to return if the task is not found in the queue. A task will
  take value `missing` if it is running, complete, errored, deferred etc
  and a positive integer if it is in the queue, indicating its position
  (with 1) being the next task to run.

- queue:

  The name of the queue to query (defaults to the "default" queue).

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

An integer vector, the same length as `task_ids`
