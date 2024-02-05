##' List all tasks.  This may be a lot of tasks, and so can be quite
##' slow to execute.
##'
##' @title List all tasks
##'
##' @param controller The controller to use.  If not given (or `NULL`)
##'   we'll use the controller registered with
##'   [rrq_register_default_controller]
##'
##' @return A character vector
##'
##' @export
rrq_task_list <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  as.character(con$HKEYS(keys$task_expr))
}
