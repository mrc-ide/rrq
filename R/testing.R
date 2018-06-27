rrq_test_data <- function(rrq, expr, envir = parent.frame(),
                          dest = "rrq_data") {
  dat <- context::prepare_expression(expr, envir, rrq$db)
  bin <- redux::object_to_bin(dat)
  keys <- rrq$keys

  txt <-
    c("# Generate a task_id (rrq uses a 32 character random hex string)",
      "",
      "# Write the binary content of the task expression to this hash",
      sprintf('# HSET "%s" "<task_id>" <binary contents>', keys$task_expr),
      sprintf('task_expr: "%s"', keys$task_expr),
      "",
      "# For completeness, set the task status",
      sprintf('# HSET "%s" "<task_id>" "PENDING"', keys$task_status),
      sprintf('task_status: "%s"', keys$task_status),
      "",
      "# Pushing the task on the queue must be the last step",
      sprintf("# RPUSH %s <task_id>", keys$queue_rrq),
      sprintf('queue: "%s"', keys$queue_rrq))

  dest_yml <- sprintf("%s.yml", dest)
  dest_bin <- sprintf("%s.bin", dest)
  message(sprintf("Writing key information to '%s'", dest_yml))
  writeLines(txt, sprintf("%s.yml", dest))
  message(sprintf("Writing task information to '%s'", dest_bin))
  writeBin(bin, sprintf("%s.bin", dest))
}


rrq_test <- function(expr, sources = NULL, packages = NULL,
                     n_workers = 0L, dest = "rrq_data", root = "context") {
  message("Setting up context at ", root)
  env <- environment()
  if (length(sources) == 0L) {
    sources <- NULL
  }
  if (length(packages) == 0L) {
    packages <- NULL
  }

  ctx <- context::context_save(root, sources = sources, packages = packages)
  ctx <- context::context_load(ctx, env)
  rrq <- rrq_controller(ctx)
  rrq_test_data(rrq, expr, env, dest)
  if (n_workers > 0L) {
    worker_spawn(rrq, n_workers)
    on.exit(rrq$worker_stop())
    message(sprintf("Ctrl-C or 'kill -SIGINT %s' to stop", Sys.getpid()))
    Sys.sleep(Inf)
  }
}
