heartbeat <- function(con, key, period) {
  if (!is.null(period)) {
    message(sprintf("Starting heartbeat thread on %s (period: %d s)",
                    key, period))
    loadNamespace("heartbeatr")
    config <- con$config()
    ret <- heartbeatr::heartbeat(key, period,
                                 host = config$host, port = config$port,
                                 password = config$password, db = config$db)
    message("...ok")
    ret
  }
}
