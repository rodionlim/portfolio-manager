#!/usr/bin/env python3

import logging
import os
import pandas as pd

from pf_manager.db import entity, dao, sqlalchemy_engine_session
from pf_manager.db.table import recreate_table
from pf_manager.db.utils import dao_entity_generators
from pf_manager.utilfns.cfg import read_cfg
from pf_manager.utilfns.log import setup_log

setup_log()
cfg = read_cfg()
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")


def generate_orm() -> None:
    for table in cfg.get("tables"):
        path = os.path.join(BASE_DIR, "templates",
                            "template_" + table + ".csv")
        dao_entity_generators(table, path=path)


def create():
    """ Create initial tables """
    for table in cfg.get("tables"):
        logging.info(f"Creating {table} in db")
        entity_name = "".join([x.capitalize() for x in table.split("_")])
        Entity = getattr(entity, entity_name)
        recreate_table(Entity)  # Delete and create database table


def seed():
    """ Seed initial tables """
    for table in cfg.get("tables"):
        # for table in cfg.get("seeded_tables"):
        logging.info(f"Seeding {table} with data")
        path = os.path.join(BASE_DIR, "templates",
                            "template_" + table + ".csv")
        entity_name = "".join([x.capitalize() for x in table.split("_")])
        Entity = getattr(entity, entity_name)
        dao_obj = getattr(dao, f'get_{entity_name}_dao')()
        data = pd.read_csv(path)

        # Convert dates to python understandable dates
        for col in ["date", "ex_date"]:
            if col in data:
                data[col] = pd.to_datetime(data[col]).apply(lambda x: x.date())
        data = data.where(pd.notnull(data), None).to_dict(orient="records")
        entities = [Entity(**x) for x in data]

        try:
            with sqlalchemy_engine_session() as session:
                logging.info("Attempting to dump entities into db")
                dao_obj.add_all(session, entities)
                session.commit()
                logging.info(
                    f"Successfully dumped {len(entities)} entities into db")
        except Exception as e:
            logging.error(f'Dumping failed. Logs: {e}')
            raise e


if __name__ == "__main__":
    # generate_orm()
    create()
    seed()