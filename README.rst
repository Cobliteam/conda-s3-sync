conda-s3-sync
=================

Synchronize Anaconda environments to/from Amazon S3


Installation
------------

Run ``pip install conda-s3-sync``, or ``python ./setup.py``

Usage
-----

::

    positional arguments:
      BUCKET[/PATH]        Bucket and path of S3 location to synchronize to/from

    optional arguments:
      -h, --help           show this help message and exit
      --path-filter REGEX  Regular expression of env paths to include
      --conda-bin PATH     Path to conda-binary
      --include-root-env   Include root Anaconda environment in addition to any
                           custom envs


AWS credentials should be set up using IAM roles, or the usual environment
variables (such as ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and
``AWS_DEFAULT_REGION``).

Operation
---------

Upon execution, the list of currently available Anaconda environments in the
local system will be gathered, as will the environments exported to S3.
Any environments that are found missing on either side will be synchronized, by
exporting the local environment and pushing the resulting YAML description to S3, or by creating a new environment from the remote description.

Similarly, if environments are present both locally and remotely,
synchronization is performed from the one modified most recently to the one
modified least recently.

Local modification time is determined (and persisted) in the
``env_path/conda-meta/history`` file modification time.

Remote modification time is stored as a custom metadata entry in the S3 objects,
as AWS does not allow setting a custom ``LastModified`` time.

License (MIT)
-------------

::

    Copyright (C) 2017 Cobli

    Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
