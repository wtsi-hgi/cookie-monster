# Base Docker Image for Cookie Monster

Build a base Docker image to run a Cookie Monster instance, with all
necessary dependencies, including baton and iRODS.

## Build Instructions

First the [docker-baton](https://github.com/wtsi-hgi/docker-baton) image
should be built using the HGI specific query fork of baton:

    docker build --build-arg BRANCH=feature/specificquery \
                 --build-arg REPOSITORY=https://github.com/wtsi-hgi/baton.git \
                 -t wtsi-hgi/baton:0.16.1-specificquery \
                 -f custom/irods-3.3.1/Dockerfile \
                 github.com/wtsi-hgi/docker-baton.git

The iRODS setup is per this image. See the respective documentation for
details. Once this is built, the Cookie Monster image can be built using
the provided `Dockerfile`:

    docker build -t wtsi-hgi/cookie-monster .

This forms a base environment for a Cookie Monster implementation. It
does *not* provide a Cookie Monster implementation.

# License

Copyright (c) 2016 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
