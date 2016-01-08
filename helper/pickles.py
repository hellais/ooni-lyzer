import helper.files
import logging
import pickle
import glob
import os


def load(path):
    if globby(path=path):
        return list(map(lambda p: pickle.load(open(p, "rb")), glob.glob(path)))
    else:
        return pickle.load(open(path, "rb"))


def save(data, path):
    logging.error("Saving to %s" % path)
    with open(path, 'wb') as fp:
        pickle.dump(data, fp)


def globby(path):
    return '*' in path
