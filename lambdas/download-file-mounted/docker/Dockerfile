FROM onedata/lambda-base-slim:v2

USER root

RUN apt update
RUN apt install -y dpkg-dev \
                   cmake \
                   g++ \
                   gcc \
                   binutils \
                   libx11-dev \
                   libxpm-dev \
                   libxft-dev \
                   libxext-dev \
                   libssl-dev \
                   git


RUN python3 -m pip install xrootd


USER app
