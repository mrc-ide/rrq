# List active workers

Returns the ids of active workers. This does not include exited workers;
use
[`rrq_worker_list_exited()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_list_exited.md)
for that.

## Usage

``` r
rrq_worker_list(controller = NULL)
```

## Arguments

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector of worker names

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_worker_list(controller = obj)
}
```
