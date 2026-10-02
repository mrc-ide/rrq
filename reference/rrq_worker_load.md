# Report on worker load

Report on worker "load" (the number of workers being used over time).
Reruns an object of class `worker_load`, for which a `mean` method
exists (this function is a work in progress and the interface may
change).

## Usage

``` r
rrq_worker_load(worker_ids = NULL, controller = NULL)
```

## Arguments

- worker_ids:

  Optional vector of worker ids. If `NULL` then all active workers are
  used.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

An object of class "worker_load", which has a pretty print method.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
mean(rrq_worker_load(controller = obj))
}
```
