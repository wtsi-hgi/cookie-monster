from testwithbaton.models import BatonDockerBuild


BATON_DOCKER_BUILD = BatonDockerBuild(
    "github.com/wtsi-hgi/docker-baton.git",
    "wtsi-hgi/baton:specificquery",
    "custom/irods-3.3.1/Dockerfile",
    {
        "REPOSITORY": "https://github.com/wtsi-hgi/baton.git",
        "BRANCH": "tmp"
    }
)
