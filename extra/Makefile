RSCRIPT = Rscript --no-init-file

all: benchmark.html

benchmark.html: benchmark.Rmd
	Rscript -e "rmarkdown::render('$<')"

clean:
	rm -f benchmark.md benchmark.html
	rm -rf figure

.SECONDARY: benchmark.md
