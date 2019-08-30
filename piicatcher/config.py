config = {}


def set_global_config(c):
    global config    # Needed to modify global copy of config
    config = c
