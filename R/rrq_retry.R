##' Retry a task (or set of tasks). Typically this is after failure
##' (e.g., `ERROR`, `DIED` or similar) but you can retry even
##' successfully completed tasks. Once retried, functions that
##' retrieve information about a task (e.g., [rrq_task_status()]`,
##' [rrq_task_result()]) will behave differently depending on the
##' value of their `follow` argument. See
##' `vignette("fault-tolerance")` for more details.
##'
##' @title Retry tasks
##'
##' @param task_ids Task ids to retry.
##'
##' @inheritParams rrq_task_list
##'
##' @return New task ids
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' # It's straightforward to see the effect of retrying a task with
##' # one that proiduces a different value each time, so here, we use a
##' # simple task that draws one normally distributed random number
##' t1 <- rrq_task_create_expr(rnorm(1), controller = obj)
##' rrq_task_wait(t1, controller = obj)
##' rrq_task_result(t1, controller = obj)
##'
##' # If we retry the task we'll get a different value:
##' t2 <- rrq_task_retry(t1, controller = obj)
##' rrq_task_wait(t2, controller = obj)
##' rrq_task_result(t2, controller = obj)
##'
##' # Once a task is retried, most of the time (by default) you can use
##' # the original id and the new one exchangeably:
##' rrq_task_result(t1, controller = obj)
##' rrq_task_result(t2, controller = obj)
##'
##' # Use the 'follow' argument to modify this behaviour
##' rrq_task_result(t1, follow = FALSE, controller = obj)
##' rrq_task_result(t2, follow = FALSE, controller = obj)
##'
##' # See the retry chain with rrq_task_info
##' rrq_task_info(t1, controller = obj)
##' rrq_task_info(t2, controller = obj)
rrq_task_retry <- function(task_ids, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_character(task_ids)
  con <- controller$con
  keys <- controller$keys

  if (anyDuplicated(task_ids) > 0) {
    stop(sprintf(
      "task_ids must not contain duplicates:\n%s",
      paste(sprintf("  - %s", unique(task_ids[duplicated(task_ids)])),
            collapse = "\n")))
  }

  chain <- task_follow_chain(controller, task_ids)
  task_ids_leaf <- vcapply(chain, last, USE.NAMES = FALSE)
  task_ids_root <- vcapply(chain, first, USE.NAMES = FALSE)

  if (anyDuplicated(task_ids_leaf)) {
    dup <- task_ids[duplicated(task_ids_leaf)]
    i <- task_ids_leaf %in% dup
    err <- vcapply(split(task_ids[i], factor(task_ids_leaf[i], dup)),
                   function(x) paste(sprintf("    - %s", x), collapse = "\n"))
    stop(sprintf(
      "task_ids must point to distinct tasks:\n%s",
      paste(sprintf("  - %s\n%s", names(err), err), collapse = "\n")))
  }

  status <- rrq_task_status(task_ids_leaf, follow = FALSE,
                            controller = controller)

  not_retriable <- !(status %in% TASK$retriable)
  if (any(not_retriable)) {
    stop(sprintf(
      "Can't retry tasks that are in state: %s:\n%s",
      paste(squote(unique(status[not_retriable])), collapse = ", "),
      paste(sprintf("  - %s", task_ids[not_retriable]), collapse = "\n")),
      call. = FALSE)
  }

  n <- length(task_ids)
  time <- timestamp()
  task_ids_new <- ids::random_id(n)

  key_queue <- rrq_key_queue(
    keys$queue_id,
    list_to_character(con$HMGET(keys$task_queue, task_ids_root)))
  if (all(key_queue == key_queue[[1]])) {
    queue_push <- list(redis$RPUSH(key_queue[[1]], task_ids_new))
  } else {
    key_queue_split <- split(task_ids_new, key_queue)
    queue_push <-
      unname(Map(redis$RPUSH, names(key_queue_split), key_queue_split))
  }

  con$pipeline(
    .commands = c(list(
      redis$HMSET(keys$task_status,      task_ids_leaf, rep(TASK_MOVED, n)),
      redis$HMSET(keys$task_status,      task_ids_new,  rep(TASK_PENDING, n)),
      redis$HMSET(keys$task_time_moved,  task_ids_leaf, rep_len(time, n)),
      redis$HMSET(keys$task_time_submit, task_ids_new,  rep_len(time, n)),
      redis$HMSET(keys$task_moved_to,    task_ids_leaf, task_ids_new),
      redis$HMSET(keys$task_moved_root,  task_ids_new,  task_ids_root),
      redis$HMSET(keys$task_expr,        task_ids_new,  task_ids_root)),
      queue_push))

  task_ids_new
}


task_follow <- function(controller, task_ids) {
  con <- controller$con
  keys <- controller$keys
  key <- keys$task_moved_to
  i <- rep_len(TRUE, length(task_ids))
  while (any(i)) {
    moved_to <- unname(from_redis_hash(con, key, task_ids[i]))
    is_terminal <- is.na(moved_to)
    i[is_terminal] <- FALSE
    task_ids[i] <- moved_to[!is_terminal]
  }
  task_ids
}


task_follow_root <- function(controller, task_ids) {
  con <- controller$con
  keys <- controller$keys
  task_ids_root <- unname(from_redis_hash(con, keys$task_moved_root, task_ids))
  task_ids_root[is.na(task_ids_root)] <- task_ids[is.na(task_ids_root)]
  task_ids_root
}


task_follow_chain <- function(controller, task_ids) {
  task_ids <- task_follow_root(controller, task_ids)
  chain <- NULL
  if (any(!is.na(task_ids))) {
    con <- controller$con
    keys <- controller$keys
    while (any(!is.na(task_ids))) {
      chain <- cbind(chain, task_ids, deparse.level = 0)
      task_ids <- unname(from_redis_hash(con, keys$task_moved_to, task_ids))
    }
  }
  lapply(seq_len(nrow(chain)), function(i) na_drop(chain[i, ]))
}


is_task_redirect <- function(x) {
  is.character(x)
}
