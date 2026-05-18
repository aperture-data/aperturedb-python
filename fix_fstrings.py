import os, re

files = [
    "aperturedb/CSVWriter.py",
    "aperturedb/CommonLibrary.py",
    "aperturedb/ParallelQuery.py",
    "aperturedb/ParallelQuerySet.py",
    "aperturedb/Query.py",
    "aperturedb/Utils.py",
    "aperturedb/cli/configure.py",
    "examples/CelebADataKaggle.py",
    "test/conftest.py",
    "test/test_Server.py",
    "test/test_Stats.py"
]

for file in files:
    with open(file, "r") as f:
        content = f.read()

    # Pattern: a multiline f-string caused by PEP 701, looking like:
    # f"something {\n    variable} something"
    # We want to replace it with:
    # (f"something {variable} something")
    # Actually, fixing it is easier by just removing the newline and spaces inside the brackets
    # and then letting autopep8 reformat? No, if we let autopep8 reformat, it will recreate them.
    # We need to wrap the whole f-string in parenthesis, or split the line before the f-string.
    
    # We can use regex to find: `assert <cond>, f".... {\n   var} ..."`
    # But wait, it's not just asserts.
    # Let's just find `f".... {\n   ...` and see where they are.
