rm -rf build/ dist/ vdms.egg-info/
python3 -m build
twine upload --skip-existing --verbose dist/*
