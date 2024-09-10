# adb : Commad line utility.

adb is a command line utility to have a well defined way of doing routine tasks with AperturDB instance.
It's based on [typer](https://typer.tiangolo.com/)

It has subcommands with their parameters defined under the cli directory.

Some key points to consider:
- Against conventions of importing different classes at module level, the functions in adb should tend to import them lazily (even at the risk of repeating). This is because the recursive imports bog the startup down, which makes for a bad user experience.

## Notes about improving the load times.
execute the command to be tested with PYTHONPROFILEIMPORTTIME set as 1
```
pip install tuna
PYTHONPROFILEIMPORTTIME=1 adb config ls 2>&1 | tee check_times
tuna check_times
```