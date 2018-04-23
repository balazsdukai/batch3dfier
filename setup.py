#!/usr/bin/python3
# -*- coding: utf-8 -*-

from setuptools import setup

setup(name='batch3dfier',
    version='0.8.0',
    description='A wrapper around 3dfier to 3dfy datasets in batch.',
    url='https://github.com/balazsdukai/batch3dfier',
    author='Balázs Dukai',
    author_email='balazs.dukai@gmail.com',
    license='GPLv3',
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
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        
        'Operating System :: POSIX :: Linux'
    ],
    python_requires='>=3',
    keywords='GIS 3DGIS CityGML LiDAR',
    entry_points={
        'console_scripts': ['batch3dfier = batch3dfier.batch3dfierapp:main',
                            'bag3d = batch3dfier.bag3d:main']
    },
    include_package_data=True,
    zip_safe=False
    )
