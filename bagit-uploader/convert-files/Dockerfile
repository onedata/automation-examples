FROM --platform=${TARGETPLATFORM:-linux/amd64} ghcr.io/openfaas/classic-watchdog:0.1.5 as watchdog
FROM --platform=${TARGETPLATFORM:-linux/amd64} ubuntu:bionic

RUN apt update
RUN apt install unoconv -y

RUN apt-get update && apt-get install -y \
    python3-pip \
    wget \
    ghostscript \
    imagemagick \
    ffmpeg



# Allows you to add additional packages via build-arg
ARG ADDITIONAL_PACKAGE

COPY --from=watchdog /fwatchdog /usr/bin/fwatchdog
RUN chmod +x /usr/bin/fwatchdog

# Add non root user
RUN addgroup --system app && adduser --system --ingroup app app
WORKDIR /home/app/

# Add pdf2archive script
RUN wget https://github.com/matteosecli/pdf2archive/archive/refs/tags/v0.3.2.tar.gz
RUN tar -xf v0.3.2.tar.gz
RUN cd pdf2archive-0.3.2/ && chmod +x pdf2archive
ENV PATH=$PATH:/home/app/pdf2archive-0.3.2


COPY index.py           .
COPY requirements.txt   .

RUN chown -R app /home/app && \
  mkdir -p /home/app/python && chown -R app /home/app
ENV PATH=$PATH:/home/app/.local/bin:/home/app/python/bin/
ENV PYTHONPATH=$PYTHONPATH:/home/app/python


RUN mkdir -p function
RUN touch ./function/__init__.py

WORKDIR /home/app/function/
COPY requirements.txt  .
RUN python3 -m pip install  -r requirements.txt -t /home/app/python

WORKDIR /home/app/
COPY function           function

# Allow any user-id for OpenShift users.
RUN chown -R app:app ./ && \
  chmod -R 777 /home/app/python

ENV fprocess="python3 index.py"
EXPOSE 8080

HEALTHCHECK --interval=3s CMD [ -e /tmp/.lock ] || exit 1

CMD ["fwatchdog"]
