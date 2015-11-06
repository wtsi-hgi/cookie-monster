FROM python:3.4-wheezy
MAINTAINER hgi@sanger.ac.uk

COPY . /cookie-monster
WORKDIR cookie-monster

RUN ./scripts/pip-install-requirements.sh

CMD ["./scripts/run-tests.sh"]