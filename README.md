# pydf
Python package for Dataflow at GCP

## Developer's Guide

### Requirements
* Python 3.9.x (3.10.x+ and 3.8.x- will not work)
    * Follow the instructions at https://github.com/pyenv/pyenv-installer, make sure that sqlite3 is installed on your local machine, then run the following
        * `pyenv install 3.9`
        * `pyenv global 3.9`

### IDE
VSCode with Code Spell Checker + autoDocstring + Python + Pylance

### Setup Environment
```bash
make init
source venv/bin/activate
```
If you want to clean up your environment,
```bash
make clean
```

Please always run these before you commit your code:
```bash
make format
make lint
```

Run tests:
```bash
make test
```

## User's Guide

To-Do.
