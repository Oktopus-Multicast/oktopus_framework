# coding=utf-8
import os
from setuptools import setup, find_packages
from setuptools.extension import Extension

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='oktopus',
    version='0.1',
    description='Oktopus Framework',
    author='Khaled Diab, Carlos Lee',
    author_email='kdiab@sfu.ca, carlosl@sfu.ca',
    package_dir={'': 'framework'},
    install_requires=required,
    packages = find_packages(),
    url='https://github.com/Oktopus-Multicast/oktopus_framework.git',
    classifiers=[
        "License :: OSI Approved :: MIT License"
    ]
)