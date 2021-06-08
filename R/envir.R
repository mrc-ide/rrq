##' Helper function for creating a worker environment. This function
##' exists to create a function suitable for passing to the `envir`
##' method of [rrq::rrq_controller()] for the common case where the
##' worker should source some R scripts and/or load some packages on
##' startup. This is a convenience wrapper around defining your own
##' function for use with the `envir` method of
##' [rrq::rrq_controller()], which covers the most simple case. If you
##' need more flexibility you should write your own.
##'
##' @title Create simple worker environments
##'
##' @param packages An optional character vector of
##'
##' @param sources An optional character vector of scripts to
##'   read. Typically these will contain just function definitions but
##'   you might read large data objects here too.
##'
##' @return A function suitable for passing to the `$envir()` method
##'   of [rrq::rrq_controller()]
##'
##' @export
rrq_envir <- function(packages = NULL, sources = NULL) {
  if (!is.null(packages)) {
    assert_character(packages)
  }
  if (!is.null(sources)) {
    assert_character(sources)
    assert_file_exists(sources)
  }

  ## We create the function with an environment that has the global
  e <- new.env(parent = globalenv())
  e$packages <- packages
  e$sources <- sources
  with(e, function(envir) {
    for (p in packages) {
      library(p, character.only = TRUE)
    }
    for (s in sources) {
      sys.source(s, envir = envir)
    }
  })
}
