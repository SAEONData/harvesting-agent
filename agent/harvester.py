from datetime import datetime, timedelta
from collections import OrderedDict
from traceback import format_exc
import json

from sqlalchemy import Column, Integer, String, Enum, DateTime
from sqlalchemy.orm import validates
from sqlalchemy.dialects.postgresql import JSONB

from .history_meta import Versioned
from .persistence import Persistent, session, transaction
from .exceptions import HarvestingError
from .collectorfactory import CollectorFactory
from .datasource import Datasource
from .harvestedrecord import HarvestedRecord
from .repository import Repository
from .curatorfactory import CuratorFactory
from .log import logger


# keys and values taken from the Plone Harvester object
# use OrderedDict to ensure the frequency Enum type is created consistently
frequencies = OrderedDict()
frequencies['Never'] = None
frequencies['60 Seconds'] = timedelta(seconds=60)
frequencies['12 Hours'] = timedelta(hours=12)
frequencies['1 Days'] = timedelta(days=1)
frequencies['2 Days'] = timedelta(days=2)
frequencies['7 Days'] = timedelta(days=7)
frequencies['14 Days'] = timedelta(days=14)
frequencies['30 Days'] = timedelta(days=30)
frequencies['6 Months'] = timedelta(days=180)
frequencies['12 Months'] = timedelta(days=360)


class Harvester(Persistent, Versioned):
    """
    The metadata harvesting 'engine'. Contains all the configuration required to fetch metadata from
    a Datasource (using a Collector) and commit it to a Repository (using a Curator). The Harvester is
    loosely coupled with Datasource and Repository, and is not bound to the records that it generates.
    """

    __tablename__ = 'harvester'

    harvester_id = Column(Integer, primary_key=True)
    uid = Column(String, unique=True, index=True, nullable=False)
    datasource_uid = Column(String, nullable=False)
    repository_uid = Column(String, nullable=False)

    protocol = Column(Enum('OPeNDAP-NetCDF', name='harvester_protocol'), nullable=False)
    schema = Column(Enum('DataCite', name='metadata_schema'), nullable=False)
    default_values = Column(JSONB(none_as_null=True), nullable=False)
    supplementary_values = Column(JSONB(none_as_null=True), nullable=False)
    granularity = Column(JSONB(none_as_null=True), nullable=False)
    search_url = Column(String, nullable=False)
    commit_url = Column(String, nullable=False)

    frequency = Column(Enum(*frequencies.keys(), name='harvester_frequency'), nullable=False)
    status = Column(Enum('Active', 'Inactive', 'Deleted', name='harvester_status'), nullable=False)
    lastrun = Column(DateTime, info={'not_versioned': ''})

    # maximum number of times to attempt some operation on a harvested record;
    # after this the operation is not retried and the record is left in an error state
    max_attempts = 10

    def __repr__(self):
        return "<Harvester:{}>".format(self.harvester_id)

    @validates('default_values', 'supplementary_values', 'granularity')
    def _validate_json(self, attr, value):
        """
        Validates the assignment of a JSON attribute, to ensure we're getting a dict.
        :param attr: attribute name
        :param value: either a JSON string or the deserialised object
        :return: dict
        """
        # any form of empty => empty dict
        if not value:
            value = {}
        elif type(value) == str:
            value = json.loads(value)
        if type(value) != dict:
            raise ValueError("{}: expecting dict; got {}".format(attr, type(value)))
        return value

    def isharvestdue(self):
        """
        Returns True if at least <frequency> time has passed since <lastrun>.
        """
        if self.status != 'Active' or frequencies[self.frequency] is None:
            return False
        if self.lastrun is None:
            return True
        return datetime.now() > self.lastrun + frequencies[self.frequency]

    def harvest(self, limit=None):
        """
        Harvest any unharvested metadata.
        :param limit: maximum number of new records to collect
        """
        if self.status != 'Active':
            raise HarvestingError("Cannot harvest: harvester status is {}".format(self.status))

        datasource = session.query(Datasource).filter(Datasource.uid == self.datasource_uid).one()
        repository = session.query(Repository).filter(Repository.uid == self.repository_uid).one()

        collector = CollectorFactory.create_collector(self.protocol, self.schema, datasource.url,
                                                      datasource.username, datasource.password)
        curator = CuratorFactory.create_curator(self.schema, self.default_values, self.supplementary_values,
                                                self.granularity, repository.url, self.search_url, self.commit_url,
                                                repository.username, repository.password, repository.institution)

        self.lastrun = datetime.now()
        self._fetch_new_records(collector, datasource, repository, limit)
        self._fetch_pending_records(collector, datasource, repository)
        self._commit_records(curator, datasource, repository)

    def _fetch_new_records(self, collector, datasource, repository, limit):
        """
        Collect newly added records from a datasource and create corresponding harvested records.
        :param collector: a Collector instance
        :param datasource: the Datasource from which records are being collected
        :param repository: the Repository for which records are being harvested
        :param limit: maximum number of new records to collect
        """
        logger.debug("Fetching new records")

        collected_records = collector.fetch_records(limit=limit)
        if not collected_records:
            return

        # the hard work has already been done by the collector, so we can wrap all inserts in a single transaction
        with transaction():
            for collected_record in collected_records:
                harvested_record_exists = session.query(
                    session.query(HarvestedRecord).\
                    filter(HarvestedRecord.datasource == datasource).\
                    filter(HarvestedRecord.repository == repository).\
                    filter(HarvestedRecord.uid == collected_record['uid']).\
                    exists()
                ).scalar()

                if not harvested_record_exists:
                    harvested_record = HarvestedRecord()
                    harvested_record.datasource = datasource
                    harvested_record.repository = repository
                    harvested_record.uid = collected_record['uid']
                    harvested_record.timestamp = collected_record['timestamp']
                    harvested_record.metadata_ = collected_record['metadata'] if collected_record['status'] == 'Success' else None
                    harvested_record.status = 'Fetched' if collected_record['status'] == 'Success' else 'Pending'
                    harvested_record.lasterror = collected_record['error'] if collected_record['status'] == 'Error' else None
                    harvested_record.errorcount = 1 if collected_record['status'] == 'Error' else 0
                    harvested_record.updated = datetime.now()
                    session.add(harvested_record)

    def _fetch_pending_records(self, collector, datasource, repository):
        """
        Fetch metadata for any records currently in the 'Pending' state.
        :param collector: a Collector instance
        :param datasource: the Datasource from which records are being collected
        :param repository: the Repository for which records are being harvested
        """
        logger.debug("Fetching pending records")

        harvested_records = session.query(HarvestedRecord).\
            filter(HarvestedRecord.datasource == datasource).\
            filter(HarvestedRecord.repository == repository).\
            filter(HarvestedRecord.status == 'Pending').\
            filter(HarvestedRecord.errorcount < self.max_attempts).\
            all()

        for harvested_record in harvested_records:
            # use a separate transaction for each record update, as the collector has to do work for each one
            with transaction():
                try:
                    collected_record = collector.fetch_metadata(harvested_record.uid)
                    harvested_record.timestamp = collected_record['timestamp']
                    if collected_record['status'] == 'Success':
                        harvested_record.metadata_ = collected_record['metadata']
                        harvested_record.status = 'Fetched'
                        harvested_record.lasterror = None
                        harvested_record.errorcount = 0
                    else:
                        harvested_record.lasterror = collected_record['error']
                        harvested_record.errorcount += 1
                except Exception as e:
                    harvested_record.lasterror = format_exc()
                    harvested_record.errorcount += 1
                harvested_record.updated = datetime.now()

    def _commit_records(self, curator, datasource, repository):
        """
        Commit the metadata of records in the 'Fetched' state to the repository.
        :param curator: a Curator instance
        :param datasource: the Datasource from which records are being collected
        :param repository: the Repository for which records are being harvested
        """
        logger.debug("Committing records")

        harvested_records = session.query(HarvestedRecord).\
            filter(HarvestedRecord.datasource == datasource).\
            filter(HarvestedRecord.repository == repository).\
            filter(HarvestedRecord.status == 'Fetched').\
            filter(HarvestedRecord.errorcount < self.max_attempts).\
            all()

        for harvested_record in harvested_records:
            # use a separate transaction for each record update, as the curator has to do work for each one
            with transaction():
                try:
                    harvested_record.metadata_uid = curator.commit_metadata(
                        harvested_record.metadata_, self.datasource_uid, harvested_record.uid)
                    harvested_record.status = 'Committed'
                    harvested_record.lasterror = None
                    harvested_record.errorcount = 0
                except Exception as e:
                    harvested_record.lasterror = format_exc()
                    harvested_record.errorcount += 1
                    logger.exception("Error committing %s: %s", harvested_record.uid, e)
                harvested_record.updated = datetime.now()
