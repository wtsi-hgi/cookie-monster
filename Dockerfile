# git clone https://github.com/wtsi-hgi/docker-baton
# cd docker-baton
# docker build -t wtsi-hgi/baton:0.16.1 -f 0.16.1/irods-3.3.1/Dockerfile .
FROM wtsi-hgi/baton:0.16.1
MAINTAINER "Human Genetics Informatics" <hgi@sanger.ac.uk>

# We can get Python 3.5 from the Deadsnakes PPA
# We match Debian Jessie with Ubuntu Trusty
COPY deadsnakes.list /etc/apt/sources.list.d/deadsnakes.list
RUN gpg --keyserver keyserver.ubuntu.com --recv-keys DB82666C \
 && gpg --export DB82666C | apt-key add - \
 && apt-get update \
 && apt-get install -y --no-install-recommends python3.5 \
 && ln -s /usr/bin/python3.5 /usr/bin/python
