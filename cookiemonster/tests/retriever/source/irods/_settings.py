"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from testwithbaton.models import BatonDockerBuild


BATON_DOCKER_BUILD = BatonDockerBuild(
    tag="wtsi-hgi/baton:specificquery",
    path="github.com/wtsi-hgi/docker-baton.git",
    docker_file="custom/irods-3.3.1/Dockerfile",
    build_args={
        "REPOSITORY": "https://github.com/wtsi-hgi/baton.git",
        "BRANCH": "feature/specificquery"
    }
)
