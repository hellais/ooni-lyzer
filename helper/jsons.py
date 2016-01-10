import logging
import json
import glob


def load(path):
    if globby(path=path):
        contents = list(map(lambda p: json.load(open(p, "r")), glob.glob(path)))
        if len(contents) == 1:
            return contents[0]
        else:
            return contents
    else:
        return json.load(open(path, "r"))


def save(data, path):
    logging.info("Saving to %s" % path)
    with open(path, 'w') as fp:
        json.dump(data, fp)


def globby(path):
    return '*' in path
