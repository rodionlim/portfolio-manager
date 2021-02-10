from abc import ABC, abstractmethod
import concurrent.futures
import datetime
from functools import partial
import logging
import pandas as pd

from pf_manager.db import sqlalchemy_engine_session
from pf_manager.db.utils import dao_entity_generators


class Dumper(ABC):
    """ 
    Dumper requires a dao and Entity object (sqlAlchemy). 
    To create these objects, use the infer_dtypes method.
    Child classes that inherit Dumper should set them in the constructor,
    and implement the abstract methods in the Dumper class.
    Methods:
    dump: Dump data to db
    dump_single: Dump single day data to db, must specify a date
    dump_all: Delete entire table before dumping data to db (Use with care)
    dump_parallel: Allow multiprocessing when reading in data, before sequentially dumping data to db
    create_table: Delete and recreate table based on the Entity (Use with care)
    infer_dtypes: dao and Entity generator, which infers schema using pandas dtype
    """
    def __init__(self, dao, Entity, db="coredb"):
        self.dao = dao
        self.Entity = Entity
        self.DB = db

    def dump(self, **kwargs) -> None:
        """ Dump multiple days of data that are not present in the database """
        dates = self._get_file_dates()
        assert isinstance(dates, set) or isinstance(dates, dict)

        if isinstance(dates, dict):
            dts = dates.copy()
            dates = set(dates.keys())
            kwargs["dts"] = dts
        db_dates = self._get_distinct_dates_from_db()
        dates = dates - db_dates

        logging.info(
            f'Detected {len(dates)} new files to be dumped into the database')

        # Begin loading and dumping sequentially - this is not done in parallel due to
        # potential issues with locking of the database rows during the writing process
        for dt in dates:
            logging.info(f"Begin loading data for {dt}")

            df = self._load_single_data(dt, **kwargs).fillna(value=0.)
            if len(df) == 0:
                logging.info(f"No data to be dumped for {dt}")
                continue

            entity_by_date = self._cvt_df_to_entities(df)
            logging.info(f"Begin dump for {dt}")
            self._dump_entities(entity_by_date)

    def dump_all(self) -> None:
        """ Use with care, this will wipe the entire table and reload everything """
        self.create_table()
        self.dump()

    def dump_parallel(self, n_workers=4, **kwargs) -> None:
        """ Dump multiple days of data that are not present in the database """
        assert n_workers <= 8  # Don't allow more than 8 processes to be spun up
        dates = self._get_file_dates()
        assert isinstance(dates, set) or isinstance(dates, dict)
        if isinstance(dates, dict):
            dts = dates.copy()
            dates = set(dates.keys())
            kwargs["dts"] = dts
        db_dates = self._get_distinct_dates_from_db()
        dates = dates - db_dates

        logging.info(
            f'Detected {len(dates)} new files to be dumped into the database')

        # Begin loading in parallel
        gen = self.chunks(dates, n_workers)

        for dates in gen:
            dts_str = ", ".join([x.strftime("%Y-%m-%d") for x in dates])
            logging.info(f'Begin loading data for {dts_str}')
            with concurrent.futures.ProcessPoolExecutor(
                    max_workers=n_workers) as executor:
                results = executor.map(
                    partial(self._load_single_data, **kwargs), dates)

            # Insertion is still sequential, probably can be in parallel too
            for df in results:
                if len(df) == 0:
                    continue
                df.fillna(0., inplace=True)
                entity_by_date = self._cvt_df_to_entities(df)
                logging.info(f"Begin dump for {dts_str}")
                self._dump_entities(entity_by_date)

    def dump_single(self, dt: datetime.date, **kwargs) -> None:
        """ Dump single day portfolio to the database """
        logging.info(f"Begin loading data for {dt}")

        df = self._load_single_data(dt, **kwargs)
        if len(df) == 0:
            logging.info(f'No data to be dumped for {dt}')
            return

        df.fillna(0., inplace=True)
        entity_by_date = self._cvt_df_to_entities(df)
        logging.info(f"Begin dump for {dt}")
        self._dump_entities(entity_by_date)

    def create_table(self) -> None:
        """ Fires a create table SQL based on Entity mapped to the broker file type """
        from pf_manager.db.table import recreate_table  # An engine object is created in this class, hence this should not be a global import
        try:
            logging.info("Creating table")
            recreate_table(self.Entity)
            logging.info("Successfully created table")
        except:
            logging.error("Failed to create table")

    @staticmethod
    def infer_dtypes(tableName: str,
                     path: str = None,
                     df: pd.DataFrame = None):
        return dao_entity_generators(tableName, path=path, df=df)

    def _get_distinct_dates_from_db(self):
        # create a list of dates that exist in the table from the db
        with sqlalchemy_engine_session(db=self.DB) as session:
            dates_in_db = {x[0] for x in self.dao.mget_distinct_dates(session)}
        return dates_in_db

    @abstractmethod
    def _get_file_dates(self) -> list:
        """ Does not have to be file dates, just a list of dates to compare against what is in the db """
        raise NotImplementedError

    @abstractmethod
    def _load_single_data(self, dt: datetime.date, **kwargs) -> pd.DataFrame:
        """ A pandas dataframe to be uploaded into the database """
        raise NotImplementedError

    def _cvt_df_to_entities(self, df: pd.DataFrame) -> list:
        res = []
        data = df.to_dict(orient="records")
        [res.append(self.Entity(**x)) for x in data]
        return res

    def _dump_entities(self, entities: list) -> None:
        try:
            with sqlalchemy_engine_session(db=self.DB) as session:
                logging.info("Attempting to dump entities into db")
                self.dao.add_by_date(session, entities)
                session.commit()
                logging.info(
                    f"Successfully dumped {len(entities)} entities into db")
        except Exception as e:
            logging.error(
                f'Dumping failed for {entities[0].filedate}. Logs: {e}')
            raise e

    @staticmethod
    def chunks(lst: list, n):
        """Yield successive n-sized chunks from a list or set."""
        if isinstance(lst, set):
            lst = list(lst)

        for i in range(0, len(lst), n):
            yield lst[i:i + n]