import subprocess

def run_fix():
    # We will just reset again and apply fixes to avoid dealing with autopep8 mess
    subprocess.run(["git", "reset", "--hard", "59ba13e"])

run_fix()
