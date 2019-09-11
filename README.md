# rrq

> Simple Redis Queue

[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![Travis-CI Build Status](https://travis-ci.org/mrc-ide/rrq.svg?branch=master)](https://travis-ci.org/mrc-ide/rrq)
[![AppVeyor Build status](https://ci.appveyor.com/api/projects/status/u8e9nulhk7ryo5jd?svg=true)](https://ci.appveyor.com/project/richfitz/rrq-xkvo8)
[![codecov.io](https://codecov.io/github/mrc-ide/rrq/coverage.svg?branch=master)](https://codecov.io/github/mrc-ide/rrq?branch=master)

Simple Redis queue in R.  This is like the bigger package `rrqueue`, but using `context` for most of the heavy lifting and aiming to be more like the lightweight parallelisation packages out there.

Once this works I'll rework `rrqueue` off of this codebase probably.

## Installation

```r
drat:::add("mrc-ide")
install.packages("rrq")
```

## Testing

To test, we need a redis server that can be automatically connected to using the `redux` defaults.  This is satisfied if you have an unauthenticated redis server running on localhost, otherwise you should update the environment variable `REDIS_URL` to point at a redis server.  Do not use a production server, as the package will create and delete a lot of keys.

A suitable redis server can be started using docker with

```
./scripts/redis start
```

(and stopped with `./scripts/redis stop`)

## License

MIT © [Rich FitzJohn](https://github.com/richfitz).
