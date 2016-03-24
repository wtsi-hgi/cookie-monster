"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import atexit
import logging
from typing import Tuple, Callable
from urllib.parse import urlparse
from uuid import uuid4

from cookiemonster.tests._utils.docker_helpers import get_open_port, get_docker_client


def setup_influxdb_in_docker(repository: str, tag: str) -> Tuple[str, int, Callable]:
    """
    Setup an InfluxDB instance in docker using the given Docker Hub repository and tag.
    :param repository: the Docker Hub repository URL
    :param tag: the Docker Hub repository tag
    :return: tuple where the first element is the location of the database, the second is the port the HTTP API runs on
    and the third is a method to tear down the database
    """
    docker_client = get_docker_client()

    response = docker_client.pull(repository, tag)
    logging.debug(response)

    http_api_port = get_open_port()

    container = docker_client.create_container(
        name="influxdb-%s" % uuid4(),
        image="%s:%s" % (repository, tag),
        ports=[http_api_port],
        host_config=docker_client.create_host_config(
            port_bindings={
                8086: http_api_port
            }
        )
    )

    # Ensure that, no matter what, the container gets killed
    def tear_down():
        docker_client.kill(container)
        atexit.unregister(tear_down)

    atexit.register(tear_down)

    docker_client.start(container)
    logging.info("Waiting for InfluxDB server to setup")
    for line in docker_client.logs(container, stream=True):
        logging.debug(line)
        if "Listening for signals" in str(line):
            break

    url = urlparse(docker_client.base_url)
    host = url.hostname if url.scheme in ["http", "https"] else "localhost"

    return host, http_api_port, tear_down
