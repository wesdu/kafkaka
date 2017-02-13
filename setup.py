# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='kafkaka',
      version='0.5.0',
      description='Kafka Client with smarter protocol described, support for Gevent and tornado',
      long_description=open('README.rst').read(),
      author='Du Wei',
      author_email='pandorid@gmail.com',
      url='https://github.com/wesdu/kafkaka',
      license="Apache License 2.0",
      classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Natural Language :: Chinese (Simplified)',
        'Natural Language :: Chinese (Traditional)',
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords='Kafka,gevent,tornado,client',
      packages=['kafkaka'],
      package_dir={'kafkaka':'kafkaka'},
      package_data={'kafkaka':['*.*']}, requires=['gevent', 'tornado']
)
