#!/usr/bin/env python

from distutils.core import setup

setup(name='hamake',
      version='1.0',
      description='Hadoop Make Utility',
      author='Vadim Zaliva',
      author_email='lord@crocodile.org',
      url='http://code.google.com/p/hamake/',

      packages = [
        'hadoopfs',
        'hamake'
      ],
      package_dir = {'hadoopfs' : 'src/hadoopfs', 'hamake' : 'src/hamake'},
      
      scripts=['scripts/hamake','src/hadoopfs/ThriftHadoopFileSystem-remote'],
      data_files=[('share/doc/hamake', ['TODO.txt','ChangeLog.txt', 'README.txt','hamakefile.xsd','sample_hamakefile.xml'])]
     )
