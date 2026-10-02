# Clean up exited workers

Cleans up workers known to have exited. See vignette("fault-tolerance")
for more details.

## Usage

``` r
rrq_worker_delete_exited(worker_ids = NULL, controller = NULL)
```

## Arguments

- worker_ids:

  Optional vector of worker ids. If `NULL` then rrq looks for exited
  workers using
  [`rrq_worker_list_exited()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_list_exited.md).
  If given, we check that the workers are known and have exited.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector of workers that were deleted

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_worker_delete_exited(controller = obj)
}
```
