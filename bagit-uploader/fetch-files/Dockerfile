FROM openfaas/classic-watchdog:0.18.1 as watchdog

FROM centos:7

# Install XrootD
RUN yum install -y epel-release.noarch
RUN yum upgrade -y
RUN yum install -y xrootd xrootd-client
RUN yum clean all

RUN yum -y install epel-release
RUN yum -y install python3 python3-pip

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