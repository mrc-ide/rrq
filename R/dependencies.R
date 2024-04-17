queue_dependencies <- function(controller, task_id, deferred_task_ids) {
  con <- controller$con
  keys <- controller$keys

  dependency_keys <- rrq_key_task_depends_up(keys$queue_id, deferred_task_ids)
  res <- con$pipeline(.commands = c(
    lapply(dependency_keys, redis$SREM, task_id),
    set_names(lapply(dependency_keys, redis$SCARD), deferred_task_ids))
  )

  ## Tasks with 0 remaining dependencies can be queued
  tasks_to_queue <- names(res[res == 0 & names(res) != ""])
  if (length(tasks_to_queue > 0)) {
    task_queues <- list_to_character(con$HMGET(keys$task_queue, tasks_to_queue))
    queue_keys <- rrq_key_queue(keys$queue_id, task_queues)
    queue_task <- function(id, queue_key) {
      list(
        redis$LPUSH(queue_key, id),
        redis$HMSET(keys$task_status, id, TASK_PENDING)
      )
    }
    cmds <- Map(queue_task, tasks_to_queue, queue_keys)
    con$pipeline(.commands = unlist(cmds, FALSE, FALSE))
  }
}


task_depends_down <- function(controller, task_ids) {
  queue_id <- controller$keys$queue_id
  key <- function(k) rrq_key_task_depends_down(queue_id, k)
  task_depends_walk(controller, key, task_ids)
}


task_depends_up <- function(controller, task_ids) {
  queue_id <- controller$keys$queue_id
  key <- function(k) rrq_key_task_depends_up_original(queue_id, k)
  task_depends_walk(controller, key, task_ids)
}


task_depends_walk <- function(controller, key, task_ids) {
  con <- controller$con
  ret <- list()
  while (length(task_ids) > 0) {
    deps <- lapply(
      con$pipeline(.commands = lapply(key(task_ids), redis$SMEMBERS)),
      list_to_character)
    i <- lengths(deps) > 0
    ret <- c(ret, set_names(deps[i], task_ids[i]))
    task_ids <- unique(unlist(deps[i]))
  }
  if (length(ret) == 0) NULL else ret
}


verify_dependencies_exist <- function(controller, depends_on) {
  if (!is.null(depends_on)) {
    dependencies_exist <- rrq_task_exists(depends_on, controller = controller)
    if (!all(dependencies_exist)) {
      missing <- names(dependencies_exist[!dependencies_exist])
      error_msg <- ngettext(
        length(missing),
        "Failed to queue as dependency %s does not exist.",
        "Failed to queue as dependencies %s do not exist.")
      stop(sprintf(error_msg, paste0(missing, collapse = ", ")))
    }
  }
  invisible(TRUE)
}
