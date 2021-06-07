FROM rocker/r-ver:3.6.1

RUN apt-get update && apt-get -y install --no-install-recommends \
        libhiredis-dev \
        libssl-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY docker/bin /usr/local/bin/
RUN sed  -i'' '/mran.microsoft.com/d' /usr/local/lib/R/etc/Rprofile.site && \
        Rscript -e 'install.packages("docopt")' && \
        install_packages --repo=https://mrc-ide.github.io/drat \
            R6 \
            docopt \
            ids \
            progress \
            redux

COPY . /src
RUN R CMD INSTALL /src && \
        Rscript -e 'rrq:::rrq_worker_script("/usr/local/bin")' && \
        rm -rf /src
