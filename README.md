[![CircleCI](https://circleci.com/gh/tokern/piicatcher.svg?style=svg)](https://circleci.com/gh/tokern/piicatcher)
[![codecov](https://codecov.io/gh/tokern/piicatcher/branch/master/graph/badge.svg)](https://codecov.io/gh/tokern/piicatcher)
[![PyPI](https://img.shields.io/pypi/v/piicatcher.svg)](https://pypi.python.org/pypi/piicatcher)
[![image](https://img.shields.io/pypi/l/piicatcher.svg)](https://pypi.org/project/piicatcher/)
[![image](https://img.shields.io/pypi/pyversions/piicatcher.svg)](https://pypi.org/project/piicatcher/)

# Pii Catcher for Files and Databases

## Overview

PiiCatcher finds PII data in your databases. It scans all the columns in your 
database and files in your storage. 

Check out [AWS Glue & Lake Formation Privilege Analyzer](https://tokern.io/blog/lake-glue-access-analyzer) for an example of how piicatcher is used in production.

## Quick Start

PiiCatcher is available as a command-line application.

To install use pip:

    python3 -m venv .env
    source .env/bin/activate
    pip install piicatcher

    # Install Spacy English package
    python -m spacy download en_core_web_sm
    
    # run piicatcher on a sqlite db and print report to console
    piicatcher db -c '/db/sqlqb'
    ╭─────────────┬─────────────┬─────────────┬─────────────╮
    │   schema    │    table    │   column    │   has_pii   │
    ├─────────────┼─────────────┼─────────────┼─────────────┤
    │        main │    full_pii │           a │           1 │
    │        main │    full_pii │           b │           1 │
    │        main │      no_pii │           a │           0 │
    │        main │      no_pii │           b │           0 │
    │        main │ partial_pii │           a │           1 │
    │        main │ partial_pii │           b │           0 │
    ╰─────────────┴─────────────┴─────────────┴─────────────╯


## Supported Technologies

PiiCatcher supports the following filesystems:
* POSIX
* AWS S3 (for files that are part of tables in AWS Glue and AWS Athena)
* Google Cloud Storage _(Coming Soon)_
* ADLS _(Coming Soon)_

PiiCatcher supports the following databases:
1. **Sqlite3** v3.24.0 or greater
2. **MySQL** 5.6 or greater
3. **PostgreSQL** 9.4 or greater
4. **AWS Redshift**
5. **SQL Server**
6. **Oracle**
7. **AWS Glue/AWS Athena**

## Documentation

For advanced usage refer documentation [at its website](https://tokern.io/docs/piicatcher).

## Contributing

For Contribution guidelines, [refer to developer documentation](https://tokern.io/docs/piicatcher/development). 

