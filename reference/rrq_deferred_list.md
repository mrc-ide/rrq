# List deferred tasks

Return deferred tasks and what they are waiting on. Note this is in an
arbitrary order, tasks will be added to the queue as their dependencies
are satisfied.

## Usage

``` r
rrq_deferred_list(controller = NULL)
```

## Arguments

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).
