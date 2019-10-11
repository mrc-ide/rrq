FROM rocker/r-ver:3.6.1

RUN apt-get update && apt-get -y install \
        libhiredis-dev

COPY docker/bin /usr/local/bin/
RUN sed  -i'' '/mran.microsoft.com/d' /usr/local/lib/R/etc/Rprofile.site

RUN install_packages \
        R6 \
        docopt \
        ids \
        progress \
        redux

RUN install_remotes \
        mrc-ide/heartbeatr

COPY . /src
RUN R CMD INSTALL /src && \
        Rscript -e 'rrq:::write_rrq_worker("/usr/local/bin")' && \
        rm -rf /src
