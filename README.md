# Metadata Harvesting Agent

## Deployment

### System dependencies
* python3 (&ge; 3.5)
* postgresql (&ge; 9.5)
* libgdal-dev

### Package dependencies
The GDAL Python bindings must be installed from the OS distribution repository,
before installing the Agent with pip:
* python3-gdal

This will ensure that the correct version of numpy is installed, i.e. with the
same header files with which the GDAL Python bindings were compiled. More info
[here](https://trac.osgeo.org/gdal/wiki/PythonGotchas#PythoncrashesinGDALfunctionswhenyouupgradeordowngradenumpy).

### Package installation
Assuming the Agent repository has been cloned to `$AGENTDIR`, install the Agent
and its remaining package dependencies with:

    pip3 install $AGENTDIR

### Database setup
Run the following commands to create a PostgreSQL user and database for the Agent:

    sudo -u postgres createuser agent --pwprompt --echo
    sudo -u postgres createdb agentdb --owner=agent --echo

Update the `DB*` values in `$AGENTDIR/config/agent.ini` as necessary. Run the
following command to initialise the database schema:

    python3 $AGENTDIR/bin/agentdb.py
