# Fetch task information

Fetch information about a task. This currently includes information
about where a task is (or was) running and information about any retry
chain, but will expand in future. The format of the output here is
subject to change (and will probably get a nice print method) but the
values present in the output will be included in any future update.

## Usage

``` r
rrq_task_info(task_id, controller = NULL)
```

## Arguments

- task_id:

  A single task identifier

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A list, format currently subject to change

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

# Get information about a task
t <- rrq_task_create_expr(runif(1), controller = obj)
rrq_task_info(t, controller = obj)

# If the task has been retried, the retry chain is shown
rrq_task_wait(t, controller = obj)
rrq_task_retry(t, controller = obj)
rrq_task_info(t, controller = obj)

# If the task was queued onto a separate process, then this
# information is shown
rrq_task_create_expr(1 + 1, separate_process = TRUE, timeout_task_run = 60,
                      controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_info(t, controller = obj)
}
```
