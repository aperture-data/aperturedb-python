rm -rf build/ dist/ vdms.egg-info/
python3 setup.py sdist bdist_wheel
twine upload --verbose dist/*
