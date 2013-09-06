# -*- coding: utf-8 -*-
#
# This file is a part of rq-arbiter released under the MIT license.
# See the NOTICE for more information.

import os
from setuptools import setup
from setuptools import find_packages

from rq_arbiter import release

HERE = os.path.dirname(__file__)
with open(os.path.join(HERE, 'README.rst')) as f:
    long_description = f.read()

with open(os.path.join(HERE, 'requirements.txt')) as f:
    requirements = f.read()

setup(
    name = release.name,
    version = release.version,
    description = release.description,
    long_description = long_description,
    author = release.author,
    author_email = release.author_email,
    license = release.license,
    url = release.url,
    zip_safe = False,
    packages = find_packages(),
    include_package_data = True,
    install_requires = requirements
)
