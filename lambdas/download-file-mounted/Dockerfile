FROM docker.onedata.org/lambda-base-slim:v1

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

RUN mkdir -p /tmp/xrootd_build ; cd /tmp/xrootd_build ; \
    git clone https://github.com/xrootd/xrootd.git ; \
    cd xrootd; git checkout v5.5.2 ; cd .. ; \
    cmake ./xrootd -DCMAKE_INSTALL_PREFIX=/opt/xrootd -DENABLE_PERL=FALSE ; \
    make ; make install; \
    echo /opt/xrootd/lib >> /etc/ld.so.conf; ldconfig; \
    cd bindings/python/; python setup.py install; \
    cd /tmp; rm -rf xrootd_build

USER app
