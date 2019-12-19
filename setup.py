from setuptools import setup, find_packages
from setuptools.command.install import install
from codecs import open
from os import path, getenv

import sys

__version__ = '0.6.1'


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = getenv('CIRCLE_TAG')

        if tag != ("v%s" % __version__):
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, __version__
            )
            sys.exit(info)


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# get the dependencies and installs
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if x.startswith('git+')]

setup(
    name='piicatcher',
    version=__version__,
    description='Find PII data in databases',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/tokern/piicatcher',
    download_url='https://github.com/tokern/piicatcher/tarball/' + __version__,
    license='Apache 2.0',
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.4',
      'Programming Language :: Python :: 3.5',
      'Programming Language :: Python :: 3.6',
      'Programming Language :: Python :: 3.7',
      'Topic :: Database',
      'Topic :: Software Development',
      'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='',
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    author='Rajat Venkatesh',
    install_requires=install_requires,
    dependency_links=dependency_links,
    author_email='piicatcher@tokern.io',
    entry_points={
        'console_scripts': ['piicatcher=piicatcher.command_line:main'],
    },
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
