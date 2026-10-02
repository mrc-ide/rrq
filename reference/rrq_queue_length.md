# Queue length

Returns the length of the queue (the number of tasks waiting to run).
This is the same as the length of the value returned by
[rrq_queue_list](https://mrc-ide.github.io/rrq/reference/rrq_queue_list.md).

## Usage

``` r
rrq_queue_length(queue = NULL, controller = NULL)
```

## Arguments

- queue:

  The name of the queue to query (defaults to the "default" queue).

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A number
