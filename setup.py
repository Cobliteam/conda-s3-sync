from setuptools import setup

VERSION = '0.2.0'

setup(
    name='conda-s3-sync',
    packages=['conda_s3_sync'],
    version=VERSION,
    description='Synchronize Anaconda environments to/from Amazon S3',
    long_description=open('README.rst').read(),
    url='https://github.com/Cobliteam/conda-s3-sync',
    download_url='https://github.com/Cobliteam/conda-s3-sync/archive/{}.tar.gz'.format(VERSION),
    author='Daniel Miranda',
    author_email='daniel@cobli.co',
    license='MIT',
    install_requires=[
        'boto3',
        'iso8601',
        'pyyaml'
    ],
    entry_points={
        'console_scripts': ['conda-s3-sync=conda_s3_sync.main:main']
    },
    keywords='conda anaconda aws s3')
