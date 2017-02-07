queue_redis <- function(context, initialise = TRUE, con = redux::hiredis()) {
  R6_queue_redis$new(context, initialise, con)
}

R6_queue_redis <- R6::R6Class(
  "queue_redis",
  inherit = queuer:::R6_queue_base,

  public = list(
    rrq = NULL,
    initialize = function(context, initialise, con) {
      super$initialize(context, initialise)
      self$rrq <- worker_controller(context$id, con)
    },
    submit = function(task_ids, names = NULL) {
      self$rrq$queue_submit(task_ids)
    },
    unsubmit = function(task_ids) {
      self$rrq$queue_unsubmit(task_ids)
    }
  ))
