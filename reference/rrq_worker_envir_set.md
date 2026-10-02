# Set worker environment

Register a function to create an environment when creating a worker.
When a worker starts, they will run this function. The function
`rrq_worker_envir_refresh` asks the worker to reload the copy of the
already specified environment (e.g., to pick up changes to source
files).

## Usage

``` r
rrq_worker_envir_set(create, notify = TRUE, controller = NULL)

rrq_worker_envir_refresh(controller = NULL)
```

## Arguments

- create:

  A function that will create an environment. It will be called with one
  parameter (an environment), in a fresh R session. The function
  [`rrq_envir()`](https://mrc-ide.github.io/rrq/reference/rrq_envir.md)
  can be used to create a suitable function for the most common case
  (loading packages and sourcing scripts). Set to `NULL` to remove
  environment creation function (i.e., to start workers with an
  essentially empty environment).

- notify:

  Boolean, indicating if we should send a `REFRESH` message to all
  workers to update their environment, via `rrq_worker_envir_refresh`

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Examples

``` r
if (FALSE) { # rrq:::enable_examples(require_queue = "rrq:example")
obj <- rrq_controller("rrq:example")

rrq_worker_envir_set(rrq_envir(packages = "ids"), controller = obj)
t <- rrq_task_create_expr(search(), controller = obj)
rrq_task_wait(t, controller = obj)
rrq_task_result(t, controller = obj)
rrq_worker_log_tail(n = 5, controller = obj)

rrq_worker_envir_set(NULL, controller = obj)
}
```
