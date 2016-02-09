# git clone https://github.com/wtsi-hgi/docker-baton
# cd docker-baton
# docker build -t wtsi-hgi/baton:0.16.1 -f 0.16.1/irods-3.3.1/Dockerfile .
FROM wtsi-hgi/baton:0.16.1
MAINTAINER "Human Genetics Informatics" <hgi@sanger.ac.uk>

# We can get Python 3.5 from the Deadsnakes PPA
# We match Debian Jessie with Ubuntu Trusty
# pip is installed per https://pip.pypa.io/en/stable/installing/
COPY deadsnakes.list /etc/apt/sources.list.d
RUN gpg --keyserver keyserver.ubuntu.com --recv-keys DB82666C \
 && gpg --export DB82666C | apt-key add - \
 && apt-get update \
 && apt-get install -y --no-install-recommends python3.5 \
 && ln -s /usr/bin/python3.5 /usr/bin/python \
 && curl https://bootstrap.pypa.io/get-pip.py | python

# Install Cookie Monster
# n.b., pip apparently won't/can't install Github-based dependencies, so
# until we figure out a better solution, we explicitly pass the contents
# of the hosted requirements.txt file to pip... This is horrible :(
ENV CM_REPO   "wtsi-hgi/cookie-monster"
ENV CM_BRANCH "develop"
RUN curl https://raw.githubusercontent.com/$CM_REPO/$CM_BRANCH/requirements.txt \
  | xargs pip install git+https://github.com/$CM_REPO.git@$CM_BRANCH#egg=cookiemonster
