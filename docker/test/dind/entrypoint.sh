echo "Starting Docker"
nohup /usr/local/bin/dockerd-entrypoint.sh > /var/log/docker.out 2> /var/log/docker.err &

echo -n "Waiting for Docker.."
dockerReady=""
while test -z "${dockerReady}"
do
    echo -n "."
    sleep 0.5 || sleep 1
    dockerReady=$(grep "Daemon has completed initialization" /var/log/docker.err)

    if grep "mount: permission denied (are you root?)" /var/log/docker.err
    then
        echo "Failed to start Docker (dumping log)"
        cat /var/log/docker.err
        exit 1
    fi
done
echo " started"

echo "Building the test runner"
docker build -t wtsi-hgi/cookie-monster/tests -f docker/test/runner/Dockerfile .

 Hack: should be possible to use this Docker as the Docker server...
echo "Starting Docker server"
docker run --privileged -d --name docker-server docker:1.9-dind
docker exec docker-server apk add --update git
echo "Docker server started"

# Hacky way of getting the address of the docker server
SERVER_ADDRESS=$(docker run --link docker-server docker:1.9-dind env | grep "DOCKER_SERVER_PORT=" | cut -f2 -d "=")
#SERVER_ADDRESS="tcp://$(ifconfig eth0 | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'):2375"

echo "Starting the test runner"
(while :; do echo "Docker container is still running..."; sleep 300; done) &
docker run --link docker-server -e DOCKER_HOST="$SERVER_ADDRESS" wtsi-hgi/cookie-monster/tests
echo "Complete"