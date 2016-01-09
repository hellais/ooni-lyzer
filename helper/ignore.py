import helper.pickles
import logging
import os


def load(path):
    ignore = set()
    if os.path.exists(path):
        existing = helper.pickles.load(path=path)
        if not isinstance(existing, set):
            logging.error("Ignore file %s is corrupt, therefore removing it" % path)
            os.remove(path)
        else:
            ignore.update(existing)
    return ignore


def update(path, data):
    logging.info("Updating ignore file %s" % path)
    helper.pickles.save(data=data, path=path)