# third party libraries
from setuptools import setup

with open("requirements.txt") as f:
    required_packages = f.read().splitlines()

setup(
    name="df",
    version="0.0.1",
    description="A useful Dataflow module",
    author="dataflow",
    packages=["df"],  # same as name
    install_requires=["wheel"] + required_packages,  # external packages as dependencies
)
