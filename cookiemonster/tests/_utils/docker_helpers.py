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
import socket
from typing import Tuple
from urllib.parse import urlparse

from docker.utils import kwargs_from_env

from docker import Client


_docker_client = None


def get_open_port() -> int:
    """
    Gets a PORT that will (probably) be available on the machine.
    It is possible that in-between the time in which the open PORT of found and when it is used, another process may
    bind to it instead.
    :return: the (probably) available PORT
    """
    free_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    free_socket.bind(("", 0))
    free_socket.listen(1)
    port = free_socket.getsockname()[1]
    free_socket.close()
    return port


def get_docker_client() -> Client:
    """
    Gets a Python client for interacting with docker. Uses the `DOCKER_HOST` environment variable to get the location of
    the daemon.
    :return: the Docker client
    """
    global _docker_client
    if _docker_client is None:
        docker_environment = kwargs_from_env(assert_hostname=False)

        if "base_url" not in docker_environment:
            raise ConnectionError("Cannot connect to Docker - is the Docker daemon running? The `DOCKER_HOST` "
                                  "environment variable should be set.")

        _docker_client = Client(**docker_environment)

    return _docker_client
