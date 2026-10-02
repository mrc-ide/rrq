# Write worker runner script

Write a small script that can be used to launch a rrq worker. The
resulting script takes the same arguments as the
[rrq_worker](https://mrc-ide.github.io/rrq/reference/rrq_worker.md)
constructor, but from the command line. See Details.

## Usage

``` r
rrq_worker_script(path, versioned = FALSE)
```

## Arguments

- path:

  The path to write to. Should be a directory (or one will be created if
  it does not yet exist). The final script will be
  `file.path(path, "rrq_worker")`

- versioned:

  Logical, indicating if we should write a versioned R script that will
  use the same path to `Rscript` as the running session. If `FALSE` we
  use `#!/usr/bin/env Rscript` which will pick up `Rscript` from the
  path. You may want to use a versioned script in tests or if you have
  multiple R versions installed simultaneously.

## Value

Invisibly, the path to the script

## Details

If you need to launch rrq workers from a script, it's convenient not to
have to embed R code like:

    Rscript -e 'rrq::rrq_worker$new("myqueue")'

as this is error-prone and unpleasant to quote and read. You can use the
function `rrq_worker_script` to write out a small helper script which
lets you write:

    ./path/rrq_worker myqueue

instead.

The helper script supports the same arguments as the `[rrq::rrq_worker]`
constructor:

- `queue_id` as the sole positional argument

- `name_config` as `--config`

- `worker_id` as `--worker-id`

To change the redis connection settings, set the `REDIS_URL` environment
variable (see
[`redux::hiredis()`](https://richfitz.github.io/redux/reference/hiredis.html)
for details).

For example to create a worker `myworker` with configuration `myconfig`
on queue `myqueue` you might use

    ./rrq_worker --config=myconfig --worker-id=myworker myqueue

## Examples

``` r
path <- rrq::rrq_worker_script(tempfile())
readLines(path)
#> [1] "#!/usr/bin/env Rscript"  "rrq:::rrq_worker_main()"
```
