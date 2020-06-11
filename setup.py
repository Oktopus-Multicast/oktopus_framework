# coding=utf-8
# from distutils.core import setup
# from distutils.extension import Extension
from setuptools import setup, find_packages
from setuptools.extension import Extension

import numpy
from Cython.Build import cythonize

# ext = [Extension('*', sources=["src/oktopus/**/*.pyx"],
#                  include_dirs=[numpy.get_include()],
#                  #  define_macros=[('CYTHON_TRACE', 1),
#                  # ('CYTHON_TRACE_NOGIL', 1)]
#                  )]

req = None
with open('requirements.txt') as r:
    req = r.readlines()

setup(
    name='oktopus',
    version='0.1',
    description='oktopus:...',


    author='Khaled Diab',
    author_email='kdiab@sfu.ca',

    package_dir={'': 'framework'},
    # packages=['oktopus_framework'],
    packages = find_packages(),
    url='https://github.com/khaledmdiab/oktopus',
    # requires=['networkx', 'requests', 'netaddr', 'numpy', 'cython', 'six',
    #           'bitstring', 'gurobi'],
    # requires=req,
    # test_requires=['pytest', 'hypothesis'],
    # ext_modules=cythonize(ext, compiler_directives={
    #     'cdivision': True,
    #     'embedsignature': True,
    #     'boundscheck': False
    # }),
    # package_data={'oktopus': ['__init__.pxd']}
)