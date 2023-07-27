import fcntl


def is_file_locked(file):
    try:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except (BlockingIOError, IOError):
        return True
    