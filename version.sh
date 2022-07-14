# Read version from python code
read_version() {
   BUILD_VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}' aperturedb/__init__.py | tr -d '"')
}