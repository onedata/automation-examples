FROM openfaas/classic-watchdog:0.18.1 as watchdog

FROM centos:7

ARG xrootd_version="4.12.2"

# Install commomn packages and Python 3
RUN rpm -Uvh http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN yum group install -y "Development Tools"
RUN yum --setopt=obsoletes=0 install -y git \
                                        wget \
                                        python3 \
                                        python3-pip \
                                        python3-devel

# Install xrootd, specific version or latest and dependencies
# cmake needed for xrootd<5.x, cmake3 for xrootd>=5.x
RUN yum-config-manager --add-repo https://xrootd.slac.stanford.edu/binaries/xrootd-stable-slc7.repo
RUN if [ ! -z "$xrootd_version" ] ; then XROOTD_V="-$xrootd_version" ; else XROOTD_V="" ; fi && \
    echo "Will install xrootd version: $XROOTD_V (latest if empty)" && \
    yum --setopt=obsoletes=0 install -y cmake \
                                        gcc-c++ \
                                        zlib-devel \
                                        openssl-devel \
                                        libuuid-devel \
                                        xrootd"$XROOTD_V"
RUN adduser --uid 1001 xrootdpyfs

# Install some prerequisites ahead of `setup.py` in order to take advantage of
# the docker build cache:
RUN pip3 install --upgrade pip setuptools
RUN pip3 install ipython \
                 pydocstyle \
                 coverage \
                 pytest \
                 pytest-pep8 \
                 pytest-cov \
                 isort \
                 mock \
                 wheel \
                 Sphinx

# Install Python xrootd and fs
# Ensure that installed version of xrootd Python client is the same as the RPM package
RUN rpm --queryformat "%{VERSION}" -q xrootd
RUN XROOTD_V=`rpm --queryformat "%{VERSION}" -q xrootd` && \
    echo "RPM xrootd version installed: $XROOTD_V" && \
    pip3 install xrootd=="$XROOTD_V" "fs<2.0"


# Allows you to add additional packages via build-arg
ARG ADDITIONAL_PACKAGE

COPY --from=watchdog /fwatchdog /usr/bin/fwatchdog
RUN chmod +x /usr/bin/fwatchdog

# Add non root user
RUN groupadd --system app && useradd --system -g app app
WORKDIR /home/app/

COPY index.py           .
COPY requirements.txt   .

RUN chown -R app /home/app && \
  mkdir -p /home/app/python && chown -R app /home/app
ENV PATH=$PATH:/home/app/.local/bin:/home/app/python/bin/
ENV PYTHONPATH=$PYTHONPATH:/home/app/python
RUN python3 -m pip install  -r requirements.txt -t /home/app/python

RUN mkdir -p function
RUN touch ./function/__init__.py

WORKDIR /home/app/function/
COPY function/requirements.txt  .


WORKDIR /home/app/
COPY function           function

# Allow any user-id for OpenShift users.
RUN chown -R app:app ./ && \
  chmod -R 777 /home/app/python

ENV fprocess="python3 index.py"
EXPOSE 8080

HEALTHCHECK --interval=3s CMD [ -e /tmp/.lock ] || exit 1

CMD ["fwatchdog"]