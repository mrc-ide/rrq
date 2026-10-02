# Detect exited workers

Detects exited workers through a lapsed heartbeat. This differs from
[`rrq_worker_list_exited()`](https://mrc-ide.github.io/rrq/reference/rrq_worker_list_exited.md)
which lists workers that have definitely exited by checking to see if
any worker that runs a heartbeat process has not reported back in time,
then marks that worker as exited. See vignette("fault-tolerance") for
details.

## Usage

``` r
rrq_worker_detect_exited(controller = NULL)
```

## Arguments

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Undefined.

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")
rrq_worker_detect_exited(controller = obj)
}
```
