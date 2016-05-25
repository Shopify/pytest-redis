#!/usr/bin/env python
from setuptools import setup


setup(
    name='pytest-redis',
    author='Samy Abidib',
    author_email='abidibs@gmail.com',
    version='0.4.9',
    py_modules=['pytest_redis'],
    url='https://github.com/sabidib/pytest-redis',
    license='MIT',
    description='A pytest plugin that pops test paths from a redis queue.',
    classifiers=[
        'License::OSI Approved::MIT License',
        'Programming Language::Python::2.7',
        'Programming Language::Python::3',
        'Operating System::OS Independent',
    ],
    install_requires=[
        'pytest==2.9.1',
        'redis==2.10.5'
    ]
)
