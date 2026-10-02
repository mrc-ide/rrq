# Fetch internal task data

Fetch internal data about a task (expert use only)

## Usage

``` r
rrq_task_data(task_id, controller = NULL)
```

## Arguments

- task_id:

  A single task identifier

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

Internal data, structures subject to change

## Examples

``` r

obj <- rrq_controller("rrq:example")

t <- rrq_task_create_expr(runif(1), controller = obj)
rrq_task_data(t, controller = obj)
#> $type
#> [1] "expr"
#> 
#> $expr
#> runif(1)
#> 
#> $variables
#> NULL
#> 

x <- 10
y <- 20
t <- rrq_task_create_expr(x + y, controller = obj)
rrq_task_data(t, controller = obj)
#> $type
#> [1] "expr"
#> 
#> $expr
#> x + y
#> 
#> $variables
#> $variables$x
#> [1] 10
#> 
#> $variables$y
#> [1] 20
#> 
#> 
```
