# Fetch task logs

Fetch logs from tasks that were queued into separate processes (e.g.,
with
[rrq_task_create_expr](https://mrc-ide.github.io/rrq/reference/rrq_task_create_expr.md)
using `separate_process = TRUE`). It is not knowable if a task
definitely produces logs - if you have a mixture of workers that do
enable task logs and some that don't, then it will depend on the worker
that picks it up if logging will be enabled. Don't do this though and
you should be fine.

## Usage

``` r
rrq_task_log(task_id, controller = NULL)
```

## Arguments

- task_id:

  A single task identifier

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector of logs, or `NULL` if no log is present yet. If
logging is not enabled for this task, we throw an error. Empty logs can
be distinguished from "no logs yet", as they will return an empty
character vector (`character(0)`).

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")

obj <- rrq_controller("rrq:example")

t <- rrq_task_create_expr(message("hello!"), separate_process = TRUE,
                          controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_log(t, controller = obj)
}
```
