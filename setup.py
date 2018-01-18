from setuptools import setup

setup(
    name='SAEON-Metadata-Agent',
    version='0.3',
    description='A metadata harvesting agent',
    url='https://github.com/SAEONData/SAEON-Metadata-Agent',
    author='Mark Jacobson',
    author_email='mark@saeon.ac.za',
    license='MIT',
    packages=['agent'],
    install_requires=[
        'python-dateutil',
        'isodate',
        'pydap',
        'requests',
        'beautifulsoup4',
        'sqlalchemy',
        'psycopg2',
        'cherrypy',
    ],
    python_requires='>=3',
    data_files=[('config', ['config/agent.ini'])],
)
