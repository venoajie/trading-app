from setuptools import setup, find_packages

setup(
    name="receiver",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "websockets>=12.0",
        "oci>=2.125",
        "uvloop>=0.19"
    ]
)