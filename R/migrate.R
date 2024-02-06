rrq_version_check <- function(con, keys, version = rrq_schema_version) {
  version_saved <- con$GET(keys$version)
  if (is.null(version_saved)) {
    ## Set rrq's version number against the data so that we know
    ## what version of the data we're talking. Later we may need to
    ## migrate old data, which will require us to detect old data
    ## and know what to move it to.
    con$SET(keys$version, version)
    return()
  } else if (version_saved == version) {
    return()
  } else if (numeric_version(version_saved) > numeric_version(version)) {
    ## This can't happen until we have a breaking change in the
    ## schema, but will work even if the user uses an old version of
    ## rrq.
    cli::cli_abort(paste(
      "rrq schema version is '{version_saved}' but you are using",
      "'{version}'; please upgrade"),
      call = NULL)
  } else {
    ## This can't happen until we have a breaking change in the
    ## schema, and of course migration is not possible (there is no
    ## migration function). But the version that introduces the
    ## breaking change will add that support.
    cli::cli_abort(paste(
      "rrq schema version is '{version_saved}' but you are using",
      "'{version}'; please migrate"),
      call = NULL)
  }
}
