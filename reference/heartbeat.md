# Create a heartbeat instance

Create a heartbeat instance

Create a heartbeat instance

## Details

Create a heartbeat instance. This can be used by running `obj$start()`
which will reset the TTL (Time To Live) on `key` every `period` seconds
(don't set this too high). If the R process dies, then the key will
expire after `3 * period` seconds (or set `expire`) and another
application can tell that this R instance has died.

## Methods

### Public methods

- [`rrq_heartbeat$new()`](#method-rrq_heartbeat-new)

- [`rrq_heartbeat$is_running()`](#method-rrq_heartbeat-is_running)

- [`rrq_heartbeat$start()`](#method-rrq_heartbeat-start)

- [`rrq_heartbeat$stop()`](#method-rrq_heartbeat-stop)

- [`rrq_heartbeat$format()`](#method-rrq_heartbeat-format)

------------------------------------------------------------------------

### Method `new()`

Create a heartbeat object

#### Usage

    rrq_heartbeat$new(
      key,
      period,
      expire = 3 * period,
      value = expire,
      config = NULL,
      start = TRUE,
      timeout = 10
    )

#### Arguments

- `key`:

  Key to use. Once the heartbeat starts it will create this key and set
  it to expire after `expiry` seconds.

- `period`:

  Timeout period (in seconds)

- `expire`:

  Key expiry time (in seconds)

- `value`:

  Value to store in the key. By default it stores the expiry time, so
  the time since last heartbeat can be computed. This will be converted
  to character with `as.character` before saving into Redis

- `config`:

  Configuration parameters passed through to
  [`redux::redis_config`](https://richfitz.github.io/redux/reference/redis_config.html).
  Provide as either a named list or a `redis_config` object. This allows
  host, port, password, db, etc all to be set.

- `start`:

  Should the heartbeat be started immediately?

- `timeout`:

  Time, in seconds, to wait for the heartbeat to appear. It should
  generally appear very quickly (within a second unless your connection
  is very slow) so this can be generally left alone.

------------------------------------------------------------------------

### Method `is_running()`

Report if heartbeat process is running. This will be `TRUE` if the
process has been started and has not stopped.

#### Usage

    rrq_heartbeat$is_running()

------------------------------------------------------------------------

### Method [`start()`](https://rdrr.io/r/stats/start.html)

Start the heartbeat process. An error will be thrown if it is already
running.

#### Usage

    rrq_heartbeat$start()

------------------------------------------------------------------------

### Method [`stop()`](https://rdrr.io/r/base/stop.html)

Stop the heartbeat process

#### Usage

    rrq_heartbeat$stop(wait = TRUE)

#### Arguments

- `wait`:

  Logical, indicating if we should wait until the heartbeat process
  terminates (should take only a fraction of a second)

------------------------------------------------------------------------

### Method [`format()`](https://rdrr.io/r/base/format.html)

Format method, used by R6 to nicely print the object

#### Usage

    rrq_heartbeat$format(...)

#### Arguments

- `...`:

  Additional arguments, currently ignored

## Examples

``` r

if (redux::redis_available()) {
  rand_str <- function() {
    paste(sample(letters, 20, TRUE), collapse = "")
  }
  key <- sprintf("rrq:heartbeat:%s", rand_str())
  h <- rrq::rrq_heartbeat$new(key, 1, expire = 2)
  con <- redux::hiredis()

  # The heartbeat key exists
  con$EXISTS(key)

  # And has an expiry of less than 2000ms
  con$PTTL(key)

  # We can manually stop the heartbeat, and 2s later the key will
  # stop existing
  h$stop()

  Sys.sleep(2)
  con$EXISTS(key) # 0

  # This is required to close any processes opened by this
  # example, normally you would not need this.
  processx:::supervisor_kill()
}
```
