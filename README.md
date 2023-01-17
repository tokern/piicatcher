[![piicatcher](https://github.com/tokern/piicatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/tokern/piicatcher/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/piicatcher.svg)](https://pypi.python.org/pypi/piicatcher)
[![image](https://img.shields.io/pypi/l/piicatcher.svg)](https://pypi.org/project/piicatcher/)
[![image](https://img.shields.io/pypi/pyversions/piicatcher.svg)](https://pypi.org/project/piicatcher/)
[![image](https://img.shields.io/docker/v/tokern/piicatcher)](https://hub.docker.com/r/tokern/piicatcher)

# PII Catcher for Databases and Data Warehouses

## Overview

PIICatcher is a scanner for PII and PHI information. It finds PII data in your databases and file systems
and tracks critical data. PIICatcher uses two techniques to detect PII:

* Match regular expressions with column names
* Match regular expressions and using NLP libraries to match sample data in columns.

Read more in the [blog post](https://tokern.io/blog/scan-pii-data-warehouse/) on both these strategies.

PIICatcher is *batteries-included* with a growing set of plugins to scan column metadata as well as metadata. 
For example, [piicatcher_spacy](https://github.com/tokern/piicatcher_spacy) uses [Spacy](https://spacy.io) to detect
PII in column data.

PIICatcher supports incremental scans and will only scan new or not-yet scanned columns. Incremental scans allow easy
scheduling of scans. It also provides powerful options to include or exclude schema and tables to manage compute resources.

There are ingestion functions for both [Datahub](https://datahubproject.io) and [Amundsen](https://amundsen.io) which will tag columns 
and tables with PII and the type of PII tags.

![PIIcatcher Screencast](https://user-images.githubusercontent.com/1638298/143765818-87c7059a-f971-447b-83ca-e21182e28051.gif)


## Resources

* [AWS Glue & Lake Formation Privilege Analyzer](https://tokern.io/blog/lake-glue-access-analyzer/) for an example of how piicatcher is used in production.
* [Two strategies to scan data warehouses](https://tokern.io/blog/scan-pii-data-warehouse/)

## Quick Start

PIICatcher is available as a docker image or command-line application.

### Installation

Docker:

    alias piicatcher='docker run -v ${HOME}/.config/tokern:/config -u $(id -u ${USER}):$(id -g ${USER}) -it --add-host=host.docker.internal:host-gateway tokern/piicatcher:latest'


Pypi:
    # Install development libraries for compiling dependencies.
    # On Amazon Linux
    sudo yum install mysql-devel gcc gcc-devel python-devel

    python3 -m venv .env
    source .env/bin/activate
    pip install piicatcher

    # Install Spacy plugin
    pip install piicatcher_spacy


### Command Line Usage
    # add a sqlite source
    piicatcher catalog add-sqlite --name sqldb --path '/db/sqldb/test.db'

    # run piicatcher on a sqlite db and print report to console
    piicatcher detect --source-name sqldb
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


### API Usage
Code Snippet: 
```python3
from dbcat.api import open_catalog, add_postgresql_source
from piicatcher.api import scan_database

# PIICatcher uses a catalog to store its state. 
# The easiest option is to use a sqlite memory database.
# For production usage check, https://tokern.io/docs/data-catalog
catalog = open_catalog(app_dir='/tmp/.config/piicatcher', path=':memory:', secret='my_secret')

with catalog.managed_session:
    # Add a postgresql source
    source = add_postgresql_source(catalog=catalog, name="pg_db", uri="127.0.0.1", username="piiuser",
                                    password="p11secret", database="piidb")
    output = scan_database(catalog=catalog, source=source)

print(output)

# Example Output
[
    ['public', 'sample', 'gender', 'PiiTypes.GENDER'],
    ['public', 'sample', 'maiden_name', 'PiiTypes.PERSON'],
    ['public', 'sample', 'lname', 'PiiTypes.PERSON'],
    ['public', 'sample', 'fname', 'PiiTypes.PERSON'],
    ['public', 'sample', 'address', 'PiiTypes.ADDRESS'],
    ['public', 'sample', 'city', 'PiiTypes.ADDRESS'],
    ['public', 'sample', 'state', 'PiiTypes.ADDRESS'], 
    ['public', 'sample', 'email', 'PiiTypes.EMAIL']
]
```

## Plugins

PIICatcher can be extended by creating new detectors. PIICatcher supports two scanning techniques:
* Metadata
* Data

Plugins can be created for either of these two techniques. Plugins are then registered using an API or using
[Python Entry Points](https://packaging.python.org/en/latest/specifications/entry-points/).

To create a new detector, simply create a new class that inherits from [`MetadataDetector`](https://github.com/tokern/piicatcher/blob/master/piicatcher/detectors.py)
or [`DatumDetector`](https://github.com/tokern/piicatcher/blob/master/piicatcher/detectors.py).

In the new class, define a function `detect` that will return a [`PIIType`](https://github.com/tokern/dbcat/blob/main/dbcat/catalog/pii_types.py) 
If you are detecting a new PII type, then you can define a new class that inherits from PIIType.

For detailed documentation, check [piicatcher plugin docs](https://tokern.io/docs/piicatcher/detectors/plugins).


## Supported Databases

PIICatcher supports the following databases:
1. **Sqlite3** v3.24.0 or greater
2. **MySQL** 5.6 or greater
3. **PostgreSQL** 9.4 or greater
4. **AWS Redshift**
5. **AWS Athena**
6. **Snowflake**

## Documentation

For advanced usage refer documentation [PIICatcher Documentation](https://tokern.io/docs/piicatcher).

## Survey

Please take this [survey](https://forms.gle/Ns6QSNvfj3Pr2s9s6) if you are a user or considering using PIICatcher. 
The responses will help to prioritize improvements to the project.

## Stats Collection
We use cookies to a analyse our traffic and features usage.
We may share information about your use of our product for our social media and marketing purposes.
These cookies don't collect your sensitive and/or confidential information.
If you would like to opt out of these cookies, run 
```bash
piicatcher --disable-stats
```
To Enable:
```bash
piicatcher --enable-stats
```

## Contributing

For Contribution guidelines, [PIICatcher Developer documentation](https://tokern.io/docs/piicatcher/development). 

