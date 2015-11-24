FROM python:3.5
MAINTAINER "hgi@sanger.ac.uk"

COPY . /cookie-monster
WORKDIR cookie-monster

RUN pip install -r requirements.txt
RUN pip install -r test_requirements.txt

CMD ["./scripts/run-tests.sh"]