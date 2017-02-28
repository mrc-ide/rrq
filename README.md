# rrq

> Simple Redis Queue

[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![Travis-CI Build Status](https://travis-ci.org/richfitz/rrq.svg?branch=master)](https://travis-ci.org/richfitz/rrq)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/richfitz/rrq?branch=master&svg=true)](https://ci.appveyor.com/project/richfitz/rrq)
[![codecov.io](https://codecov.io/github/richfitz/rrq/coverage.svg?branch=master)](https://codecov.io/github/richfitz/rrq?branch=master)

Simple Redis queue in R.  This is like the bigger package `rrqueue`, but using `context` for most of the heavy lifting and aiming to be more like the lightweight parallelisation packages out there.

Once this works I'll rework `rrqueue` off of this codebase probably.

## Installation

```r
drat:::add("dide-tools")
install.packages("rrq")
```

## License

MIT + file LICENSE © [Rich FitzJohn](https://github.com/richfitz).
