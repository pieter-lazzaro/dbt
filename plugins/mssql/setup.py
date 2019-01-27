#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-mssql"
package_version = "0.13.0a1"
description = """The mssql adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description_content_type=description,
    author="Pieter Lazzaro",
    author_email="pieter.lazzaro@pureharvest.com.au",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/mssql/dbt_project.yml',
            'include/mssql/macros/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'pyodbc>=4.0.25',
    ]
)
