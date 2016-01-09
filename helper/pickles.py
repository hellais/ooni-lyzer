import logging
import pickle
import glob


def load(path):
    if globby(path=path):
        contents = list(map(lambda p: pickle.load(open(p, "rb")), glob.glob(path)))
        if len(contents) == 1:
            return contents[0]
        else:
            return contents
    else:
        return pickle.load(open(path, "rb"))


def save(data, path):
    logging.info("Saving to %s" % path)
    with open(path, 'wb') as fp:
        pickle.dump(data, fp)


def globby(path):
    return '*' in path
