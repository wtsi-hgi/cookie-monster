FROM python:3.5
MAINTAINER "Colin Nolan" <hgi@sanger.ac.uk>

# Copy files
COPY requirements.txt test_requirements.txt /cookie-monster/
WORKDIR cookie-monster

# Install dependencies
RUN pip install -r requirements.txt
RUN pip install -r test_requirements.txt

# Install application source
COPY . /cookie-monster
CMD ["./scripts/run-tests.sh"]
