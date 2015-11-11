FROM python:3.5.0
MAINTAINER "hgi@sanger.ac.uk"

COPY . /cookie-monster
WORKDIR cookie-monster

RUN ./scripts/pip-install-requirements.sh

CMD ["./scripts/run-tests.sh"]