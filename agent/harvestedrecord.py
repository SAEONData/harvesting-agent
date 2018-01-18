from sqlalchemy import Column, Integer, String, ForeignKey, Enum, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

from .persistence import Persistent


class HarvestedRecord(Persistent):
    """
    Represents a metadata record (to be) harvested from a particular datasource and (to be)
    committed to a particular repository.
    """

    __tablename__ = 'harvestedrecord'

    datasource_id = Column(Integer, ForeignKey('datasource.datasource_id'), primary_key=True)
    repository_id = Column(Integer, ForeignKey('repository.repository_id'), primary_key=True)
    uid = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    metadata_ = Column('metadata', JSONB(none_as_null=True))
    metadata_uid = Column(String)
    status = Column(Enum('Pending', 'Fetched', 'Committed', name='harvestedrecord_status'), nullable=False)
    lasterror = Column(String)
    errorcount = Column(Integer, nullable=False)
    updated = Column(DateTime, nullable=False)

    datasource = relationship('Datasource', backref='harvested_records')
    repository = relationship('Repository', backref='harvested_records')
