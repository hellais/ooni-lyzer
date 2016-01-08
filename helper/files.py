import os


def set_extension(path, ext):
    if not ext.startswith('.'):
        ext = '.' + ext
    return os.path.splitext(path)[0] + ext