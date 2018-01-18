from . import config
from .log import logger
from .persistence import transaction, session
from .cms import CMS
from .harvester import Harvester
from .datasource import Datasource
from .repository import Repository


class Agent:
    """
    Acts as a mediator between the outside world and the metadata harvesting framework.
    """

    @staticmethod
    def invoke_harvester(harvester_uid, datasource_uid, repository_uid,
                         repository_url, username, password, institution):
        """
        Invoke a harvester.
        :param harvester_uid: identifies the harvester to invoke
        :param datasource_uid: identifies the datasource from which to collect metadata
        :param repository_uid: identifies the repository to which to commit metadata
        :param repository_url: repository URL
        :param username: username for authenticating to the repository
        :param password: password for authenticating to the repository
        :param institution: the institution that owns the repository
        :return: tuple(success:bool, message:str)
        """
        logger.debug("BEGIN invoke(harvester_uid=%s, datasource_uid=%s, repository_uid=%s, ...)",
                     harvester_uid, datasource_uid, repository_uid)
        try:
            try:
                Agent._refresh_config(harvester_uid, datasource_uid, repository_uid,
                                      repository_url, username, password, institution)
            except Exception as e:
                msg = "Error refreshing config: {}".format(e)
                logger.exception(msg)
                return False, msg

            try:
                harvester = session.query(Harvester).filter(Harvester.uid == harvester_uid).one()
                if harvester.isharvestdue():
                    logger.info("Running {}".format(harvester))
                    harvester.harvest(limit=1)
                    msg = "Finished running {}".format(harvester)
                else:
                    msg = "Not running {} as harvest is not due".format(harvester)

                logger.info(msg)
                return True, msg

            except Exception as e:
                msg = "Error running harvester: {}".format(e)
                logger.exception(msg)
                return False, msg

        finally:
            logger.debug("END invoke(harvester_uid=%s, datasource_uid=%s, repository_uid=%s, ...)",
                         harvester_uid, datasource_uid, repository_uid)

    @staticmethod
    def _refresh_config(harvester_uid, datasource_uid, repository_uid,
                        repository_url, username, password, institution):
        """
        Update harvester, datasource and repository info in the local DB to mirror the config in Plone.
        """
        cms = CMS(config.CMS_URL, username, password)
        harvester_config = cms.get_harvester_config(harvester_uid)

        def get_or_create(cls, uid):
            """
            :return: tuple(object, isnew)
            """
            obj = session.query(cls).filter(cls.uid == uid).one_or_none()
            if obj is None:
                obj = cls()
                obj.uid = uid
                session.add(obj)
                logger.debug("Creating %s with uid %s", obj, uid)
                return obj, True
            else:
                logger.debug("Found %s with uid %s", obj, uid)
                return obj, False

        with transaction():
            harvester, isnewharvester = get_or_create(Harvester, harvester_uid)

            if harvester.datasource_uid is not None and harvester.datasource_uid != datasource_uid:
                logger.warning("%s.datasource_uid has changed", harvester)
            if harvester.repository_uid is not None and harvester.repository_uid != repository_uid:
                logger.warning("%s.repository_uid has changed", harvester)

            harvester.datasource_uid = datasource_uid
            harvester.repository_uid = repository_uid
            harvester.protocol = harvester_config['transport']
            harvester.schema = harvester_config['standard']
            harvester.default_values = harvester_config['defaultValues']
            harvester.supplementary_values = harvester_config['supplementaryValues']
            harvester.granularity = harvester_config['granularity']
            harvester.frequency = harvester_config['updatefrequency']
            harvester.search_url = harvester_config['searchUrl']
            harvester.commit_url = harvester_config['commitUrl']
            harvester.status = 'Active'

            datasource, isnewdatasource = get_or_create(Datasource, datasource_uid)
            datasource.url = harvester_config['url']
            datasource.username = harvester_config['username']
            datasource.password = harvester_config['password']

            repository, isnewrepository = get_or_create(Repository, repository_uid)
            repository.url = repository_url
            repository.username = username
            repository.password = password
            repository.institution = institution

        if isnewharvester:
            logger.info("Created %s", harvester)
        if isnewdatasource:
            logger.info("Created %s", datasource)
        if isnewrepository:
            logger.info("Created %s", repository)
