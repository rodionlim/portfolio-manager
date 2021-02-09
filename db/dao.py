from datetime import datetime

from sqlalchemy import distinct

from brahms.db import entity, utils


class BrokerFilesBaseDao(object):
    def __init__(self, Entity):
        self.Entity = Entity

    def add(self, session, entry):
        session.add(entry)

    def add_all(self, session, entries: list):
        # Always use add_by_date since it prevents duplicates, only use this if bulk uploading
        session.add_all(entries)

    def add_by_date(self, session, entries: list):
        entry_dt = {x.filedate for x in entries}
        if len(entry_dt) != 1:
            raise Exception(
                "There should only be one file date present in the list of records"
            )
        else:
            filedate = entry_dt.pop()
            self.delete_by_date(session, filedate)
            self.add_all(session, entries)

    def mget_all(self, session):
        return self.mget_all_custom(session)[0].all()

    def mget_all_custom(self, session):
        """ Use this for custom queries, eg. chaining .filter_by or .filter on the results        
        """
        return (session.query(self.Entity), self.Entity)

    def mget_distinct_dates(self, session):
        return session.query(distinct(self.Entity.filedate)).all()

    def mget_by_date(self, session, dt):
        return self.mget_by_dates_custom(session, dt, dt)[0].all()

    def mget_by_dates_custom(self, session, sdt, edt):
        """ Use this for custom queries, to chain further methods on """
        return (session.query(self.Entity).filter(
            self.Entity.filedate.between(sdt.strftime('%Y-%m-%d'),
                                         edt.strftime('%Y-%m-%d'))),
                self.Entity)

    def delete_by_date(self, session, dt):
        session.query(self.Entity).filter(
            self.Entity.filedate.between(
                dt.strftime('%Y-%m-%d'),
                dt.strftime('%Y-%m-%d'))).delete(synchronize_session=False)

    def delete_all(self, session):
        session.query(self.Entity).delete(synchronize_session=False)