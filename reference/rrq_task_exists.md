# Test if tasks exist

Test if task ids exist (i.e., are known to this controller). Nonexistent
tasks may be deleted, known to a different controller or just never have
existed.

## Usage

``` r
rrq_task_exists(task_ids, named = FALSE, controller = NULL)
```

## Arguments

- task_ids:

  Vector of task ids to check

- named:

  Logical, indicating if the return value should be named with the task
  ids; as these are quite long this can make the value a little awkward
  to work with.

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A logical vector the same length as task_ids; `TRUE` where the task
exists, `FALSE` otherwise. If `named` was `TRUE`, then this vector is
named with `task_ids`.

## Examples

``` r
obj <- rrq_controller("rrq:example")

t1 <- rrq_task_create_expr(runif(1), controller = obj)
rrq_task_exists(t1, controller = obj)
#> [1] TRUE

t2 <- ids::random_id()
rrq_task_exists(t2, controller = obj)
#> [1] FALSE
```
