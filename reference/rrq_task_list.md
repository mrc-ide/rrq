# List all tasks

List all tasks. This may be a lot of tasks, and so can be quite slow to
execute.

## Usage

``` r
rrq_task_list(controller = NULL)
```

## Arguments

- controller:

  The controller to use. If not given (or `NULL`) we'll use the
  controller registered with
  [`rrq_default_controller_set()`](https://mrc-ide.github.io/rrq/reference/rrq_default_controller_set.md).

## Value

A character vector

## Examples

``` r

obj <- rrq_controller("rrq:example")

rrq_task_list(controller = obj)
#> [1] "3372332056d815a6c00094faa573549d" "fb959ba564f7ba7fbda2c7244f144cc2"
#> [3] "14afae2c120b290644738d4ef9bc067b"
```
