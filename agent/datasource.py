from sqlalchemy import Column, Integer, String

from .history_meta import Versioned
from .persistence import Persistent


class Datasource(Persistent, Versioned):
    """
    Represents a datastore from which metadata records may be harvested.
    """

    __tablename__ = 'datasource'

    datasource_id = Column(Integer, primary_key=True)
    uid = Column(String, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False)
    username = Column(String)
    password = Column(String)

    def __repr__(self):
        return "<Datasource:{}>".format(self.datasource_id)
