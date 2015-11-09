'''
Database Models
===============

foo...

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from cookiemonster.common.sqlalchemy_enum import DeclEnum

class JobStatus(DeclEnum):
    '''
    Enumeration of job states
    '''
    waiting     = 'A', 'Waiting'
    in_progress = 'B', 'In Progress'
    completed   = 'C', 'Completed'
    failed      = 'X', 'Failed'

SAModel = declarative_base()

class SAFileUpdates(SAModel):
    '''
    Model of file updates received for processing
    '''
    __tablename__  = 'file_updates'
    file_id        = Column(Integer, primary_key=True)
    file_location  = Column(String, nullable=False)
    file_hash      = Column(String, nullable=False)
    file_timestamp = Column(DateTime, nullable=False)

class SAJobs(SAModel):
    '''
    Model of processing jobs
    '''
    __tablename__ = 'jobs'
    job_id        = Column(Integer, primary_key=True)
    file_id       = Column(Integer, ForeignKey('file_updates.file_id'))
    status        = Column(JobStatus.db_type())
    last_updated  = Column(DateTime, nullable=False)

    # TODO Do I even need this?
    file_update = relationship('SAFileUpdates', backref=backref('jobs', order_by=job_id))
