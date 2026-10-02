# Cancel a task

Cancel a single task. If the task is `PENDING` it will be unqueued and
the status set to `CANCELED`. If `RUNNING` then the task will be stopped
if it was set to run in a separate process (i.e., queued with
`separate_process = TRUE`). Dependent tasks will be marked as
impossible.

## Usage

``` r
rrq_task_cancel(task_id, wait = TRUE, timeout_wait = 10, controller = NULL)
```

## Arguments

- task_id:

  Id of the task to cancel

- wait:

  Wait for the task to be stopped, if it was running.

- timeout_wait:

  Maximum time, in seconds, to wait for the task to be cancelled by the
  worker.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Nothing if successfully cancelled, otherwise throws an error with
task_id and status e.g. Task 123 is not running (MISSING)

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

t <- rrq_task_create_expr(Sys.sleep(4), separate_process = TRUE,
                          controller = obj)
Sys.sleep(0.5)
rrq_task_cancel(t, controller = obj)
rrq_task_status(t, controller = obj)
}
```
