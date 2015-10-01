"""
Copyright (C) 2014  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of cookie-monster

cookie-monster is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
This file has been created on Nov 6, 2014
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


Base = declarative_base()


class Sample(Base):
    __tablename__ = 'current_samples'

    internal_id = Column(Integer, primary_key=True)
    name = Column(String)
    accession_number = Column(String)
    organism = Column(String)
    common_name = Column(String)
    taxon_id = Column(String)
    gender = Column(String)
    ethnicity = Column(String)
    cohort = Column(String)
    country_of_origin = Column(String)
    geographical_region = Column(String)

    is_current = Column(Integer)

    def __eq__(self, other):
        return self.name == other.name and \
               self.accession_number == other.accession_number and \
               self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name)+", accession_number="+str(self.accession_number) + " }"

    def __repr__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name)+", accession_number="+str(self.accession_number) + " }"


class Study(Base):
    __tablename__ = 'current_studies'

    internal_id = Column(Integer, primary_key=True)
    name = Column(String)
    accession_number = Column(String)
    study_type = Column(String)
    description = Column(String)
    study_title = Column(String)
    study_visibility = Column(String)
    faculty_sponsor = Column(String)

    is_current = Column(Integer)

    def __eq__(self, other):
        return self.name == other.name and \
               self.accession_number == other.accession_number and \
               self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name)+", accession_number="+str(self.accession_number) + " }"

    def __repr__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name)+", accession_number="+str(self.accession_number) + " }"


class Library(Base):
    __tablename__ = 'current_library_tubes'

    internal_id = Column(Integer, primary_key=True)
    name = Column(String)
    library_type = Column(String)

    is_current = Column(Integer)

    def __eq__(self, other):
        return self.name == other.name and self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name) + " }"

    def __repr__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name) + " }"


class Wells(Base):
    __tablename__ = 'current_wells'

    internal_id = Column(Integer, primary_key=True)
    name = Column(String)

    is_current = Column(Integer)

    def __eq__(self, other):
        return self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.internal_id)

    def __str__(self):
        return "{ internal_id="+ str(self.internal_id) + "}"

    def __repr__(self):
        return self.__str__()


class MultiplexedLibrary(Base):
    __tablename__ = 'current_multiplexed_library_tubes'

    internal_id = Column(Integer, primary_key=True)
    name = Column(String)

    is_current = Column(Integer)

    def __eq__(self, other):
        return self.name == other.name and self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name) + " }"

    def __repr__(self):
        return "{ internal_id="+ str(self.internal_id)+", name="+str(self.name) + " }"


class StudySamplesLink(Base):
    __tablename__ = 'current_study_samples'

    internal_id = Column(Integer, primary_key=True)
    sample_internal_id = Column(Integer)
    study_internal_id = Column(Integer)

    is_current = Column(Integer)

    def __eq__(self, other):
        return self.sample_internal_id == other.sample_internal_id and self.internal_id == other.internal_id

    def __hash__(self):
        return hash(self.sample_internal_id) + hash(self.study_internal_id)

    def __str__(self):
        return "{ sample_internal_id="+ str(self.sample_internal_id)+", study_internal_id="+str(self.study_internal_id) + " }"

    def __repr__(self):
        return self.__str__()
