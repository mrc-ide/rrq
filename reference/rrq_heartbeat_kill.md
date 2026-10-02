# Kill a process running a heartbeat

Send a kill signal (typically `SIGTERM`) to terminate a process that is
running a heartbeat. This is used by
[`rrq_controller`](https://mrc-ide.github.io/rrq/reference/rrq_controller.md)
in order to tear down workers, even if they are processing a task. When
a heartbeat process is created, in its main loop it will listen for
requests to kill via this function and will forward them to the worker.
This is primarily useful where workers are on a different physical
machine to the controller where
[`tools::pskill()`](https://rdrr.io/r/tools/pskill.html) cannot be used.

## Usage

``` r
rrq_heartbeat_kill(con, key, signal = tools::SIGTERM)
```

## Arguments

- con:

  A hiredis object

- key:

  The heartbeat key

- signal:

  A signal to send (typically
  [`tools::SIGTERM`](https://rdrr.io/r/tools/pskill.html) for a "polite"
  shutdown)

## Examples

``` r
if (redux::redis_available()) {
  rand_str <- function() {
    paste(sample(letters, 20, TRUE), collapse = "")
  }
  # Suppose we have a process that exposes a heartbeat running on
  # this key:
  key <- sprintf("rrq:heartbeat:%s", rand_str())

  # We can send it a SIGTERM signal over redis using:
  con <- redux::hiredis()
  rrq::rrq_heartbeat_kill(con, key, tools::SIGTERM)
}
```
