import datetime
import logging
import sys
import threading

from os.path import split, basename, join

LEVELS = {
    logging.CRITICAL: "CRI",
    logging.ERROR: "ERR",
    logging.WARN: "WRN",
    logging.INFO: "INF",
    logging.DEBUG: "DBG"
}

FORMAT = '{shortlevel} {asctime} {thread} {fileline:<30}{message}'


class Formatter(logging.Formatter):
    def __init__(self):
        super(Formatter, self).__init__()
        self._fmt = FORMAT

    def format(self, record):
        record.shortlevel = LEVELS[record.levelno]
        record.message = record.getMessage()
        record.asctime = datetime.datetime.fromtimestamp(
            record.created).strftime('%Y-%m-%d %H:%M:%S.%f')
        record.thread = threading.current_thread().ident

        # to avoid the lengthy pathname, this will show the last directory + file name.
        paths = split(record.pathname)
        record.fileline = join(basename(paths[0]), paths[1]) + ':' + str(
            record.lineno) + '>'
        s = self._fmt.format(**record.__dict__)

        # Exception formatting taken from logging.Formatter
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            try:
                s = s + record.exc_text
            except UnicodeError:
                # Sometimes filenames have non-ASCII chars, which can lead
                # to errors when s is Unicode and record.exc_text is str
                # See issue 8924.
                # We also use replace for when there are multiple
                # encodings, e.g. UTF-8 for the filesystem and latin-1
                # for a script. See issue 13232.
                s = s + record.exc_text.decode(sys.getfilesystemencoding(),
                                               'replace')
        return s


def setup_log(level=logging.DEBUG, sqlalchemy_level=logging.INFO):
    # TODO(x) file handler
    handler = logging.StreamHandler()
    handler.setFormatter(Formatter())
    logging.root.addHandler(handler)
    logging.root.setLevel(level)
    logging.logThreads = 0
    logging.logProcesses = 0
    logging.logMultiprocessing = 0

    if sqlalchemy_level is not None:
        # hardcode sqlalchemy.engine to INFO, to log sql statements. (DEBUG will show result set, which will be huge).
        logging.getLogger('sqlalchemy.engine').setLevel(sqlalchemy_level)