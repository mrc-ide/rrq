##' @title Compatibility functions
##' @param ... Arguments passed to the preferred function
##' @export
##' @keywords internal
##' @rdname compat
rrq_controller2 <- function(...) {
  deprecated("rrq_controller2", "rrq_controller")
  rrq_controller(...)
}


##' @export
##' @keywords internal
##' @rdname compat
rrq_worker_config_save2 <- function(...) {
  deprecated("rrq_worker_config_save2", "rrq_worker_config_save")
  rrq_worker_config_save(...)
}


##' @export
##' @keywords internal
##' @rdname compat
rrq_worker_spawn2 <- function(...) {
  deprecated("rrq_worker_spawn2", "rrq_worker_spawn")
  rrq_worker_spawn(...)
}

rrq_configure <- function(...) {
  cli::cli_warn(
    paste("'rrq_configure()' is deprecated and does nothing. Disk offload",
          "should be configured on the controller and worker instead."),
    .frequency = "regularly",
    .frequency_id = "deprecated_rrq_configure")
}


deprecated <- function(old, new) {
  cli::cli_warn(
    "'{old}()' is deprecated, please use '{new}()' instead",
    .frequency = "regularly",
    .frequency_id = sprintf("deprecated_%s", old))
}
