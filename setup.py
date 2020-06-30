# coding=utf-8
from setuptools import setup, find_packages
from setuptools.extension import Extension

setup(
    name='oktopus',
    version='0.1',
    description='Oktopus Framework',
    author='Khaled Diab, Carlos Lee',
    author_email='kdiab@sfu.ca, carlosl@sfu.ca',
    package_dir={'': 'framework'},
    packages = find_packages(),
    url='https://github.com/Oktopus-Multicast/oktopus_framework.git',
    classifiers=[
        "License :: OSI Approved :: MIT License"
    ]
)