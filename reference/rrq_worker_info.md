# Worker information

Returns a list of information about active workers (or exited workers if
`worker_ids` includes them).

## Usage

``` r
rrq_worker_info(worker_ids = NULL, controller = NULL)
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

A list of `worker_info` objects

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_worker_info(controller = obj)
}
```
