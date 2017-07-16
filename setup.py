#!/usr/bin/python3
# -*- coding: utf-8 -*-
from setuptools import setup

setup(name='batch3dfier',
      version='0.2',
      description='A wrapper around 3dfier to 3dfy datasets in batch. ',
      url='https://github.com/balazsdukai/batch3dfier',
      author='Balázs Dukai',
      author_email='balazs.dukai@gmail.com',
      license='GPL-3.0',
      packages=['batch3dfier'],
      classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: GIS',

        # Pick your license as you wish (should match "license" above)
         'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.5',

        'Operating System :: POSIX :: Linux'
      ],
      python_requires='>=3',
      keywords='GIS 3DGIS CityGML LiDAR',
      zip_safe=False)
