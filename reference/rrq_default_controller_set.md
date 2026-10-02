# Register default controller

Set or clear a default controller for use with rrq functions. You will
want to use this to avoid passing `controller` in as a named argument to
every function.

## Usage

``` r
rrq_default_controller_set(controller)

rrq_default_controller_clear()
```

## Arguments

- controller:

  An rrq_controller object, or `NULL` to clear the default controller
  (equivalent to using `rrq_default_controller_clear`)

## Value

Invisibly, the previously set default controller (or `NULL` if none was
set)
