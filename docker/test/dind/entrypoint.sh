# Start Docker server within this Docker (hack: & used but dangerous as may not have started when required!)
/usr/local/bin/dockerd-entrypoint.sh &

# Setup Docker server (hack: should be possible to use this Docker as the server)
docker run --privileged -d --name docker-server docker:1.9-dind
docker exec docker-server apk add --update git

# Hacky way of getting the address of the docker server
SERVER_ADDRESS=$(docker run --link docker-server docker:1.9-dind env | grep "DOCKER_SERVER_PORT=" | cut -f2 -d "=")

# Build then run the test runner
docker build -t wtsi-hgi/cookie-monster/tests -f docker/test/runner/Dockerfile .
docker run --link docker-server -e DOCKER_HOST="$SERVER_ADDRESS" wtsi-hgi/cookie-monster/tests