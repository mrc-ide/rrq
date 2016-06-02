## TODO: come up with a way of scheduling object deletion.  Things
## that are created here should be deleted immediately after the
## function ends (perhaps on exit).  *Objects* should only be deleted
## if they have no more dangling pointers.
##
## So we'll register "groups" and schedule prefix deletion once the
## group is done.  But for now, don't do any of that.
prepare_expression <- function(expr, envir, db) {
  fun <- expr[[1]]
  args <- expr[-1]

  ## See context::store_expression for dealing with this properly.
  ## With a small amount of tweaking that could serialise the entire
  ## call into the context db, or just the locals.  That would
  ## probably be the right way forward because we'd get speed and not
  ## too bad a hit.  There will need to be some tweaking of the code
  ## there though because store_expression is also where the id is
  ## assigned and here we don't want to do that (in fact we don't
  ## actually have an ID?).  But we can store these by value pretty
  ## happily.

  ## TODO: This could be optimised for the many-expression case by
  ## flagging which arguments are to be ignored; then we examine just
  ## one object.
  is_call <- vlapply(args, is.call)
  is_symbol <- vlapply(args, is.symbol)

  symbols <- vcapply(unname(as.list(args))[is_symbol], as.character)
  if (any(is_call)) {
    symbols <- union(symbols,
                     unname(unlist(lapply(args[is_call], find_symbols))))
  }

  ret <- list(expr=expr)

  if (length(symbols) > 0L) {
    local <- exists(symbols, envir, inherits=FALSE)
    if (any(!local)) {
      test <- symbols[!local]
      ## TODO: Doing this *properly* requires that we know what was
      ## created in the context.  So this is going to probably copy
      ## too much over I think.  But distinguishing between Global
      ## environment variables that were created when the context was
      ## set up and from variables that have been changed is
      ## challenging.
      global <- exists(test, parent.env(.GlobalEnv))
      if (any(!global)) {
        stop("not all objects found: ",
             paste(test[!global], collapse=", "))
      }
    }

    ## NOTE: The advantage of saving these via the store is we can do
    ## deduplicated storage (which would be good if we had large
    ## objects and we get lots of duplicate objects with things like
    ## qlapply) but the (big) disadvantage is that it leads to a lot
    ## of files kicking around which is problematic from a cleanup
    ## perspective.
    if (any(local)) {
      ret$objects <- vcapply(symbols[local], function(i)
        db$set_by_value(get(i, envir, inherits=FALSE), namespace="objects"))
    }
  }

  ret
}

restore_expression <- function(dat, envir, db) {
  if (!is.null(dat$objects)) {
    db$export(envir, dat$objects, "objects")
  }
  dat$expr
}

## TODO: from context:
find_symbols <- function(expr, hide_errors=TRUE) {
  symbols <- character(0)

  f <- function(e) {
    if (!is.recursive(e)) {
      if (!is.symbol(e)) { # A literal of some type
        return()
      }
      symbols <<- c(symbols, deparse(e))
    } else {
      for (a in as.list(e[-1])) {
        if (!missing(a)) {
          f(a)
        }
      }
    }
  }

  f(expr)
  unique(symbols)
}
