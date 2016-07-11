## In a departure from how things have been arranged so far, pushing a
## bit harder for composition here; this is a special worker
## controller thing that we'll compose into the main queue.

.R6_worker_controller <- R6::R6Class(
  "worker_controller",
  public=list(
    con=NULL,
    keys=NULL,
    initialize=function(con, queue_name) {
      self$con <- con
      self$keys <- rrq_keys(queue_name)
    },

    destroy=function(delete=TRUE, type="message") {
      rrq_clean(self$con, self$context$id, delete, type)
      ## render the controller useless:
      self$context <- NULL
      self$con <- NULL
      self$keys <- NULL
    },

    ## These ones probably aren't totally needed, but are quite nice
    ## to have:
    queue_length=function() {
      self$con$LLEN(self$keys$queue_ctx)
    },
    queue_list=function() {
      as.character(self$con$LRANGE(self$keys$queue_ctx, 0, -1))
    },

    ## These ones do the actual submission
    queue_submit=function(task_ids) {
      self$con$RPUSH(self$keys$queue_ctx, task_ids)
    },
    queue_unsubmit=function(task_ids) {
      ## NOTE: probable race condition, consider rename, which has bad
      ## properties if the queue is lost of course.  The correct way
      ## would be to do a lua script.
      ids_queued <- self$queue_list()
      if (length(ids_queued) > 0L) {
        self$con$DEL(self$keys$queue_ctx)
        ids_keep <- setdiff(ids_queued, task_ids)
        if (length(ids_keep) > 0L) {
          self$con$RPUSH(self$keys$queue_ctx, ids_keep)
        }
      }
    },

    ## The messaging system from rrqueue, verbatim:
    send_message=function(command, args=NULL, worker_ids=NULL) {
      send_message(self$con, self$keys, command, args, worker_ids)
    },
    has_responses=function(message_id, worker_ids=NULL) {
      has_responses(self$con, self$keys, message_id, worker_ids)
    },
    has_response=function(message_id, worker_id) {
      has_response(self$con, self$keys, message_id, worker_id)
    },
    get_responses=function(message_id, worker_ids=NULL, delete=FALSE, wait=0) {
      get_responses(self$con, self$keys, message_id, worker_ids, delete, wait)
    },
    get_response=function(message_id, worker_id, delete=FALSE, wait=0) {
      get_response(self$con, self$keys, message_id, worker_id, delete, wait)
    },
    response_ids=function(worker_id) {
      response_ids(self$con, self$keys, worker_id)
    },

    ## Query workers:
    workers_len=function() {
      workers_len(self$con, self$keys)
    },
    workers_list=function() {
      workers_list(self$con, self$keys)
    },
    workers_list_exited=function() {
      workers_list_exited(self$con, self$keys)
    },
    workers_info=function(worker_ids=NULL) {
      workers_info(self$con, self$keys, worker_ids)
    },
    workers_status=function(worker_ids=NULL) {
      workers_status(self$con, self$keys, worker_ids)
    },
    workers_log_tail=function(worker_ids=NULL, n=1) {
      workers_log_tail(self$con, self$keys, worker_ids, n)
    },
    workers_task_id=function(worker_ids=NULL) {
      workers_task_id(self$con, self$keys, worker_ids)
    },
    workers_delete_exited=function(worker_ids=NULL) {
      workers_delete_exited(self$con, self$keys, worker_ids)
    },

    workers_stop=function(worker_ids=NULL, type="message", wait=0) {
      workers_stop(self$con, self$keys, worker_ids, type, wait)
    }
  ))
