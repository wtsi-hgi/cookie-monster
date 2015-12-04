FROM debian:jessie
MAINTAINER "hgi@sanger.ac.uk"

# Install dependencies
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates \
                                               curl \
                                               build-essential \
                                               libmozjs185-dev \
                                               libicu-dev \
                                               erlang-dev \
                                               erlang-nox

# Download source and verify signature
RUN curl -L http://www.apache.org/dist/couchdb/source/1.6.1/apache-couchdb-1.6.1.tar.gz -o couchdb.tar.gz \
 && curl -L https://www.apache.org/dist/couchdb/source/1.6.1/apache-couchdb-1.6.1.tar.gz.asc -o couchdb.tar.gz.asc \
 && curl -L https://www.apache.org/dist/couchdb/KEYS -o KEYS \
 && gpg --import KEYS \
 && gpg --verify couchdb.tar.gz.asc \
 && mkdir -p /usr/src/couchdb \
 && tar -xf couchdb.tar.gz -C /usr/src/couchdb --strip-components=1 \
 && rm -rf couchdb.tar.gz* KEYS

# Compile and install
WORKDIR /usr/src/couchdb
RUN ./configure \
 && make \
 && make install

# Expose
RUN sed -i 's/^bind_address = .*$/bind_address = 0.0.0.0/' /usr/local/etc/couchdb/default.ini
EXPOSE 5984

CMD ["couchdb"]
