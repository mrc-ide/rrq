# List tasks ahead of a task

List the tasks in front of `task_id` in the queue. If the task is
missing from the queue this will return NULL. If the task is next in the
queue this will return an empty character vector.

## Usage

``` r
rrq_task_preceeding(task_id, queue = NULL, follow = NULL, controller = NULL)
```

## Arguments

- task_id:

  Task to find the position for.

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
