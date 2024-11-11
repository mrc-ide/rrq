PACKAGE := $(shell grep '^Package:' DESCRIPTION | sed -E 's/^Package:[[:space:]]+//')
RSCRIPT = Rscript

all: install

test:
	${RSCRIPT} -e 'library(methods); devtools::test()'

roxygen:
	@mkdir -p man
	${RSCRIPT} -e "library(methods); devtools::document()"

install:
	R CMD INSTALL .

build:
	R CMD build .

check:
	_R_CHECK_CRAN_INCOMING_=FALSE make check_all

check_all:
	${RSCRIPT} -e "rcmdcheck::rcmdcheck(args = c('--as-cran', '--no-manual'))"

README.md: README.Rmd
	Rscript -e "options(warnPartialMatchArgs=FALSE); knitr::knit('$<')"
	sed -i.bak 's/[[:space:]]*$$//' README.md
	rm -f $@.bak myfile.json

clean:
	rm -f ${PACKAGE}_*.tar.gz
	rm -rf ${PACKAGE}.Rcheck


vignettes/%.Rmd: vignettes_src/%.Rmd
	mkdir -p vignettes
	cd vignettes_src && ${RSCRIPT} -e 'knitr::knit("$*.Rmd", "../vignettes/$*.Rmd")'
	sed -i.bak 's/[[:space:]]*$$//' $@
	rm -f $@.bak

vignettes: vignettes/rrq.Rmd vignettes/messages.Rmd vignettes/fault-tolerance.Rmd
	Rscript -e 'library(methods); devtools::build_vignettes()'

.PHONY: all test document install vignettes
