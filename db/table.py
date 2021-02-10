""" This script creates the tables in the database based on the object classes 
Entity refers to any of the entities in pf_manager.db.entity module
"""

from pf_manager.db import sqlalchemy_engine

engine = sqlalchemy_engine()  # Make this global


def create_table(Entity):
    """ Entity is one of the entities in brahms.db.entity """
    Entity.__table__.create(bind=engine, checkfirst=True)


def drop_table(Entity):
    """ Entity is one of the entities in brahms.db.entity. Be careful with this fn! """
    Entity.__table__.drop(bind=engine, checkfirst=True)


def recreate_table(Entity, drop_table_flag=True):
    if drop_table_flag:
        drop_table(Entity)
    create_table(Entity)