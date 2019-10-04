## Try to work out what version of a function we are likely to have
## remotely.  Some of this could nodoubt be done with rlang, but with
## ~400 functions in that package and the crazy rate at which they
## deprecate and change I thin that would be more work in the long
## run.  This used to work quite well with lazyeval but that also
## changed behaviour and is itself basically deprecated in favour of
## rlang.
match_fun_envir <- function(fun, envir = parent.frame(), envir_base = NULL) {
  fun_lazy <- substitute(fun)
  if (is_call(fun_lazy, quote(quote))) {
    fun_lazy <- fun_lazy[[2L]]
  }

  if (is.symbol(fun_lazy)) {
    fun_forced <- deparse(fun_lazy)
  } else {
    fun_forced <- force(fun)
  }

  ## This ensures that we actually have a function that we can use in
  ## all cases, though the the value found here is not used in all
  ## branches below
  fun_value <- match_fun(fun_forced, envir)

  if (is_namespaced_call(fun_lazy)) {
    return(list(value = fun_lazy, type = "name"))
  }

  if (is.character(fun_forced)) {
    name <- fun_forced
    if (!is.null(envir_base) && identical(get(name, envir_base), fun_value)) {
      return(list(value = as.name(name), type = "name"))
    } else {
      return(list(value = fun_value, type = "value"))
    }
  }

  return(list(value = fun_value, type = "value"))
}


match_fun <- function(fun, envir) {
  if (is.function(fun)) {
    fun
  } else if (is.character(fun)) {
    get(fun, mode = "function", envir = envir)
  } else if (is_call(fun, quote(`::`))) {
    getExportedValue(deparse(fun[[2]]), deparse(fun[[3]]))
  } else if (is_call(fun, quote(`:::`))) {
    get(deparse(fun[[3]]), envir = asNamespace(deparse(fun[[2]])),
        mode = "function", inherits = FALSE)
  } else {
    stop("Could not find function")
  }
}


is_namespaced_call <- function(x) {
  is.call(x) && any(deparse(x[[1]]) == c("::", ":::"))
}
