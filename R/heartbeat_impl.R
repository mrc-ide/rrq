##' Create a heartbeat instance.  This can be used by running
##' `obj$start()` which will reset the TTL (Time To Live) on `key` every
##' `period` seconds (don't set this too high).  If the R process
##' dies, then the key will expire after `3 * period` seconds (or
##' set `expire`) and another application can tell that this R
##' instance has died.
##'
##' @title Create a heartbeat instance
##'
##' @export
##' @examples
##'
##' if (redux::redis_available()) {
##'   rand_str <- function() {
##'     paste(sample(letters, 20, TRUE), collapse = "")
##'   }
##'   key <- sprintf("rrq:heartbeat:%s", rand_str())
##'   h <- rrq::heartbeat$new(key, 1, expire = 2)
##'   con <- redux::hiredis()
##'
##'   # The heartbeat key exists
##'   con$EXISTS(key)
##'
##'   # And has an expiry of less than 2000ms
##'   con$PTTL(key)
##'
##'   # We can manually stop the heartbeat, and 2s later the key will
##'   # stop existing
##'   h$stop()
##'
##'   Sys.sleep(2)
##'   con$EXISTS(key) # 0
##'
##'   # This is required to close any processes opened by this
##'   # example, normally you would not need this.
##'   processx:::supervisor_kill()
##' }
##' @importFrom R6 R6Class
##' @rdname heartbeat
heartbeat <- R6::R6Class(
  "heartbeat",

  cloneable = FALSE,

  public = list(
    ##' @description Create a heartbeat object
    ##'
    ##' @param key Key to use. Once the heartbeat starts it will
    ##'   create this key and set it to expire after `expiry` seconds.
    ##'
    ##' @param period Timeout period (in seconds)
    ##'
    ##' @param expire Key expiry time (in seconds)
    ##'
    ##' @param value Value to store in the key.  By default it stores the
    ##'   expiry time, so the time since last heartbeat can be computed.
    ##'   This will be converted to character with `as.character`
    ##'   before saving into Redis
    ##'
    ##' @param config Configuration parameters passed through to
    ##'   `redux::redis_config`.  Provide as either a named list or a
    ##'   `redis_config` object.  This allows host, port, password,
    ##'   db, etc all to be set.  Socket connections (i.e., using
    ##'   `path` to access Redis over a socket) are not currently
    ##'   supported.
    ##'
    ##' @param start Should the heartbeat be started immediately?
    ##'
    ##' @param timeout Time, in seconds, to wait for the heartbeat to
    ##'   appear.  It should generally appear very quickly (within a
    ##'   second unless your connection is very slow) so this can be
    ##'   generally left alone.
    initialize = function(key, period, expire = 3 * period,
                          value = expire, config = NULL,
                          start = TRUE, timeout = 10) {
      assert_scalar_character(key)
      assert_scalar(value) # will be converted to character
      assert_scalar_positive_integer(expire)
      assert_scalar_positive_integer(period)
      assert_scalar_logical(start)
      assert_valid_timeout(timeout)

      if (expire <= period) {
        stop("expire must be longer than period")
      }

      private$config <- redux::redis_config(config = config)

      private$key <- key
      private$key_signal <- heartbeat_key_signal(key)
      private$value <- as.character(value)

      private$period <- as.integer(period)
      private$expire <- as.integer(expire)

      private$timeout <- timeout

      if (start) {
        self$start()
      }
    },

    ##' @description Report if heartbeat process is running. This will be
    ##' `TRUE` if the process has been started and has not stopped.
    is_running = function() {
      if (is.null(private$process)) {
        FALSE
      } else {
        private$process$is_alive()
      }
    },

    ##' @description Start the heartbeat process. An error will be thrown
    ##' if it is already running.
    start = function() {
      if (self$is_running()) {
        stop(sprintf("Already running on key '%s'", private$key))
      }

      private$process <- heartbeat_process(
        private$config, private$key, private$value,
        private$period, private$expire)

      con <- redux::hiredis(private$config)
      wait_timeout("Did not start in time", private$timeout, function() {
        if (!private$process$is_alive()) {
          private$process$get_result()
        }
        con$EXISTS(private$key) == 0
      })

      invisible(self)
    },

    ##' @description Stop the heartbeat process
    ##' @param wait Logical, indicating if we should wait until the
    ##' heartbeat process terminates (should take only a fraction of a
    ##' second)
    stop = function(wait = TRUE) {
      assert_scalar_logical(wait)
      if (!self$is_running()) {
        stop(sprintf("Heartbeat not running on key '%s'", private$key))
      }

      con <- redux::hiredis(private$config)
      con$RPUSH(private$key_signal, 0)

      process <- private$process
      private$process <- NULL

      if (wait) {
        wait_timeout("Did not stop in time", private$timeout, function()
          process$is_alive())
      }

      invisible(self)
    },

    ##' @description Format method, used by R6 to nicely print the object
    ##' @param ... Additional arguments, currently ignored
    format = function(...) {
      c("<heartbeat>\n",
        sprintf("  - running: %s", tolower(self$is_running())),
        sprintf("  - key: %s", private$key),
        sprintf("  - period: %d", private$period),
        sprintf("  - expire: %d", private$expire),
        sprintf("  - redis:\n%s",
                paste0("      ", capture.output(print(private$config))[-1],
                       collapse = "\n")))
    }
  ),

  private = list(
    config = NULL,
    process = NULL,
    key = NULL,
    key_signal = NULL,
    period = NULL,
    expire = NULL,
    timeout = NULL,
    value = NULL
  ))


##' Sends a signal to a heartbeat process that is using key `key`
##' @title Send a signal
##' @param key The heartbeat key
##' @param signal A signal to send (e.g. `tools::SIGINT` or
##'   `tools::SIGKILL`)
##' @param con A hiredis object
##' @export
##' @examples
##' if (redux::redis_available()) {
##'   rand_str <- function() {
##'     paste(sample(letters, 20, TRUE), collapse = "")
##'   }
##'   # Suppose we have a process that exposes a heartbeat running on
##'   # this key:
##'   key <- sprintf("rrq:heartbeat:%s", rand_str())
##'
##'   # We can send it an interrupt over redis using:
##'   con <- redux::hiredis()
##'   rrq::heartbeat_send_signal(con, key, tools::SIGINT)
##' }
heartbeat_send_signal <- function(con, key, signal) {
  assert_scalar_character(key)
  con$RPUSH(heartbeat_key_signal(key), signal)
  invisible()
}


heartbeat_key_signal <- function(key) {
  paste0(key, ":signal")
}


heartbeat_process <- function(config, key, value, period, expire) {
  args <- list(config = config, key = key, value = value,
               period = period, expire = expire, parent = Sys.getpid())
  callr::r_bg(function(...) heartbeat_thread(...),
              args = args, package = TRUE, supervise = TRUE)
}


heartbeat_thread <- function(config, key, value, period, expire, parent) {
  con <- redux::hiredis(config)
  con$SET(key, value)
  on.exit(con$DEL(key))
  key_signal <- heartbeat_key_signal(key)
  con$DEL(key_signal)

  repeat {
    con$EXPIRE(key, expire)
    ans <- con$BLPOP(key_signal, period)
    if (!is.null(ans)) {
      value <- ans[[2L]]
      if (value %in% c(tools::SIGKILL, tools::SIGTERM)) {
        con$DEL(c(key, key_signal))
        tools::pskill(parent, value)
      } else if (value == tools::SIGINT) {
        tools::pskill(parent, value)
      }
      break
    }
  }
}