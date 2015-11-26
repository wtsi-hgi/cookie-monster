FROM python:3.5
MAINTAINER "hgi@sanger.ac.uk"

# Copy just the requirements first, so this can be cached and doesn't
# have to be rebuilt every time the application code changes
COPY requirements.txt test_requirements.txt /cookie-monster/
WORKDIR cookie-monster

# Install dependencies
RUN pip install -r requirements.txt
RUN pip install -r test_requirements.txt

# Install application source
COPY . /cookie-monster
CMD ["./scripts/run-tests.sh"]
