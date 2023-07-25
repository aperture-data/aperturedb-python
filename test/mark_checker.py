# mark_checker.py - checks if a mark is enabled
from pytest import Session, Config, Item

local_config = None


def store_config(config):
    global local_config
    local_config = config


def is_enabled(test_mark):
    global local_config
    # create a config just using commandline args.
    c = local_config.fromdictargs(dict(), local_config.invocation_params.args)
    #raise Exception( str(c.getini("filterwarnings")) + " vs " + str(local_config.getini("filterwarnings")))
    s = Session.from_config(c)
    test_item = Item.from_parent(s, name= f"modulehere_test_{test_mark}")
    required = 0
    # add marker with given mark to this test.
    if isinstance(test_mark, (list, tuple)):
        for iter_mark in test_mark:
            test_item.add_marker(iter_mark)
        required = len(test_mark)
    else:
        test_item.add_marker(test_mark)
        required = 1
    items = [test_item]
    # pytest/mark modifies items based on supplied args.
    local_config.hook.pytest_collection_modifyitems(
        session=s, config=c, items=items)
    # if item is remaining, test would be run.
    return len(items) == 1
