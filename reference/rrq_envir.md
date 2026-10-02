# Create simple worker environments

Helper function for creating a worker environment. This function exists
to create a function suitable for passing to
[rrq_worker_envir_set](https://mrc-ide.github.io/rrq/reference/rrq_worker_envir_set.md)
for the common case where the worker should source some R scripts and/or
load some packages on startup. This is a convenience wrapper around
defining your own function, covering the most simple case. If you need
more flexibility you should write your own function.

## Usage

``` r
rrq_envir(packages = NULL, sources = NULL)
```

## Arguments

- packages:

  An optional character vector of

- sources:

  An optional character vector of scripts to read. Typically these will
  contain just function definitions but you might read large data
  objects here too.

## Value

A function suitable for passing to
[rrq_worker_envir_set](https://mrc-ide.github.io/rrq/reference/rrq_worker_envir_set.md),
which can set (or update) the environment for your workers.
