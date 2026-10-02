# Read worker process log

Return the contents of a worker's process log, if it is located on the
same physical storage (including network storage) as the controller.
This will generally behave for workers started with
[rrq_worker_spawn](https://mrc-ide.github.io/rrq/reference/rrq_worker_spawn.md)
but may require significant care otherwise.

## Usage

``` r
rrq_worker_process_log(worker_id, controller = NULL)
```

## Arguments

- worker_id:

  The worker id for which the log is required

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector, one line per line in the log. If logging is enabled
but the worker has not produced any logs, this will be an empty
character vector. If logging is not enabled, then this function will
throw.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
worker_id <- rrq_worker_list(controller = obj)[[1]]
tryCatch(
  rrq_worker_process_log(worker_id, controller = obj),
  error = identity)
}
```
