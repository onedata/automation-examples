FROM onedata/lambda-base-slim:v2

USER root

RUN apt-get update
RUN apt-get install -y dpkg-dev \
                   cmake \
                   libxft-dev \
                   libssl-dev

RUN python3 -m pip install xrootd


USER app
