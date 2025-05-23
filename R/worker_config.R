##' Create a worker configuration, suitable to pass into
##' [rrq::rrq_worker_config_save]. The results of this function
##' should not be modified.
##'
##' @title Create worker configuration
##'
##' @param queue Optional character vector of queues to listen on for
##'   tasks. There is a default queue which is always listened on
##'   (called 'default'). You can specify additional names here and
##'   tasks put onto these queues with `rrq_task_create_expr()` (or
##'   other functions) will have *higher* priority than the
##'   default. You can explicitly list the "default" queue (e.g.,
##'   `queue = c("high", "default", "low")`) to set the position of
##'   the default queue.
##'
##' @param verbose Logical, indicating if the worker should print
##'   logging output to the screen.  Logging to screen has a small but
##'   measurable performance cost, and if you will not collect system
##'   logs from the worker then it is wasted time.  Logging to the
##'   redis server is always enabled.
##'
##' @param logdir Optional log directory to use for writing logs when
##'   queuing tasks in a separate process.  If given, then logs will
##'   be saved to `<logdir>/<task_id>`.  This directory should be
##'   writable by all workers and readable by the controller.
##'
##' @param poll_queue Polling time for new tasks on the queue or
##'   messages. Longer values here will reduce the impact on the
##'   database but make workers less responsive to being killed with
##'   an interrupt (control-C or Escape).  The default should be good
##'   for most uses, but shorter values are used for
##'   debugging. Importantly, longer times here do not increase the
##'   time taken for a worker to detect new tasks.
##'
##' @param timeout_idle Optional timeout that sets the length of time
##'   after which the worker will exit if it has not processed a task.
##'   This is (roughly) equivalent to issuing a `TIMEOUT_SET`
##'   message after initialising the worker, except that it's
##'   guaranteed to be run by all workers.
##'
##' @param poll_process Polling time indicating how long to wait for a
##'   background process to produce stdout or stderr. Only used for
##'   tasks queued with `separate_process` `TRUE`.
##'
##' @param timeout_process_die Timeout indicating how long to wait
##'   wait for the background process to respond to SIGTERM, either as
##'   we stop a worker or cancel a task. Only used for tasks queued
##'   with `separate_process` `TRUE`. If your tasks may take several
##'   seconds to stop, you may want to increase this to ensure a clean
##'   exit.
##'
##' @param heartbeat_period Optional period for the heartbeat.  If
##'   non-NULL then a heartbeat process will be started (using
##'   [`rrq::rrq_heartbeat`]) which can be used to build fault
##'   tolerant queues. See `vignette("fault-tolerance")` for
##'   details. If `NULL` (the default), then no heartbeat is
##'   configured.
##'
##' @param offload_threshold_size The object size beyond which task results are
##'   offloaded to disk instead of being stored in Redis. See
##'   [`rrq::rrq_controller`] for details. If `Inf` (the default), results are
##'   never offloaded.
##'
##' @return A list of values with class `rrq_worker_config`; these
##'   should be considered read-only, and contain only the validated
##'   input parameters.
##'
##' @export
##' @examples
##' rrq::rrq_worker_config()
rrq_worker_config <- function(queue = NULL, verbose = TRUE, logdir = NULL,
                              poll_queue = NULL, timeout_idle = Inf,
                              poll_process = 1, timeout_process_die = 2,
                              heartbeat_period = NULL,
                              offload_threshold_size = Inf) {
  if (is.null(queue)) {
    queue <- QUEUE_DEFAULT
  } else {
    assert_character(queue)
    if (!(QUEUE_DEFAULT %in% queue)) {
      queue <- c(queue, QUEUE_DEFAULT)
    }
  }
  assert_scalar_logical(verbose)
  if (!is.null(logdir)) {
    assert_scalar_character(logdir)
  }
  if (is.null(poll_queue)) {
    poll_queue <- if (rlang::is_interactive()) 5 else 60
  }
  assert_scalar_numeric(poll_queue)
  assert_scalar_numeric(timeout_idle)
  assert_scalar_numeric(poll_process)
  assert_scalar_numeric(timeout_process_die)
  if (!is.null(heartbeat_period)) {
    assert_scalar_numeric(heartbeat_period)
  }
  if (!is.null(offload_threshold_size)) {
    assert_scalar_numeric(offload_threshold_size)
  }
  ret <- list(queue = queue,
              verbose = verbose,
              logdir = logdir,
              poll_queue = poll_queue,
              timeout_idle = timeout_idle,
              poll_process = poll_process,
              timeout_process_die = timeout_process_die,
              heartbeat_period = heartbeat_period,
              offload_threshold_size = offload_threshold_size)
  class(ret) <- "rrq_worker_config"
  ret
}
