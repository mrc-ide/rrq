# List queue contents

Returns the keys in the task queue.

## Usage

``` r
rrq_queue_list(queue = NULL, controller = NULL)
```

## Arguments

- queue:

  The name of the queue to query (defaults to the "default" queue).

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).
