# Remove task ids from a queue

Remove task ids from a queue.

## Usage

``` r
rrq_queue_remove(task_ids, queue = NULL, controller = NULL)
```

## Arguments

- task_ids:

  Task ids to remove

- queue:

  The name of the queue to query (defaults to the "default" queue).

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).
