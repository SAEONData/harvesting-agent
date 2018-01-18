#!/usr/bin/env python
from agent.persistence import Persistent, engine

# load the metadata for our data model
from agent import datasource, repository, harvester, harvestedrecord


if __name__ == "__main__":

    phrase = "I am Chuck Norris and I eat Agents for breakfast."
    info = """
    You are about to destroy the Agent's database, and re-create it from scratch.
    Are you *absolutely* sure you want to do this?
    Type the phrase "{}" to confirm.
    
    > """.format(phrase)

    if input(info) == phrase:
        Persistent.metadata.drop_all(bind=engine)
        Persistent.metadata.create_all(bind=engine)
    else:
        print("\nYou chickened out. Wise move.")
