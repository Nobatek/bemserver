#!/usr/bin/env python3
"""BEMServer"""

from setuptools import setup, find_packages

# Get the long description from the README file
with open("README.rst", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="bemserver",
    version="0.1",
    description="BEMServer",
    long_description=long_description,
    # url="",
    author="Nobatek/INEF4",
    author_email="jlafrechoux@nobatek.com",
    # license="",
    # keywords=[
    # ],
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    install_requires=[
        "flask>=1.1.0",
        "python-dotenv>=0.9.0",
        "psycopg2>=2.8.0",
        "sqlalchemy>=1.4.0",
        "marshmallow>=3.10.0,<4.0",
        "marshmallow-sqlalchemy>=0.24.0",
        "flask_smorest>=0.29.0<0.30",
        "pandas>=1.2.3",
    ],
    packages=find_packages(exclude=["tests*"]),
)
