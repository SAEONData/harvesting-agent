from sqlalchemy import Column, Integer, String

from .history_meta import Versioned
from .persistence import Persistent


class Repository(Persistent, Versioned):
    """
    Represents a destination for harvested metadata records.
    """

    __tablename__ = 'repository'

    repository_id = Column(Integer, primary_key=True)
    uid = Column(String, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False)
    username = Column(String)
    password = Column(String)
    institution = Column(String)

    def __repr__(self):
        return "<Repository:{}>".format(self.repository_id)
