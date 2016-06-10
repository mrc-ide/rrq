RSCRIPT = Rscript --no-init-file

all: benchmark.html

%.Rmd: %.R
	${RSCRIPT} -e 'library(sowsear); sowsear("$<", output="$@")'
%.md: %.Rmd
	${RSCRIPT} -e "knitr::knit('$<')"
	sed -i.bak 's/[[:space:]]*$$//' $@
	rm -f $@.bak
%.html: %.md
	${RSCRIPT} -e 'markdown::markdownToHTML("$<", "$@")'

clean:
	rm -f benchmark.Rmd benchmark.md benchmark.html
	rm -rf figure

.SECONDARY: benchmark.md
