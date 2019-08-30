[![Build Status](https://travis-ci.org/dblintio/piicatcher.svg?branch=master)](https://travis-ci.org/dblintio/piicatcher)
[![codecov](https://codecov.io/gh/dblintio/piicatcher/branch/master/graph/badge.svg)](https://codecov.io/gh/dblintio/piicatcher)

Pii Catcher for Files and Databases
===================================

Overview
--------

PiiCatcher finds PII data in your databases. It scans all the columns in your 
database and finds the following types of PII information:
* PHONE
* EMAIL
* CREDIT_CARD
* ADDRESS
* PERSON
* LOCATION
* BIRTH_DATE
* GENDER
* NATIONALITY
* IP_ADDRESS
* SSN
* USER_NAME
* PASSWORD

PiiCatcher uses two types of scanners to detect PII information:
1. [CommonRegex](https://github.com/madisonmay/CommonRegex) uses a set of regular expressions 
for common types of information
2. [Spacy Named Entity Recognition](https://spacy.io/usage/linguistic-features#named-entities) 
uses Natural Language Processing to detect named entities. Only English language is currently supported.

Supported Technologies
----------------------
PiiCatcher supports the following filesystems:
* POSIX
* AWS S3 _(Coming soon)_
* Google Cloud Storage _(Coming Soon)_
* ADLS _(Coming Soon)_

PiiCatcher supports the following databases:
1. **Sqlite3** v3.24.0 or greater
2. **MySQL** 5.6 or greater
3. **PostgreSQL** 9.4 or greater
4. **AWS Redshift**

Installation
------------
PiiCatcher is available as a command-line application.

To install use pip:

    python3 -m venv .env
    source .env/bin/activate
    pip install piicatcher


Or clone the repo:

    git clone https://github.com/vrajat/piicatcher.git
    python3 -m venv .env
    source .env/bin/activate
    python setup.py install
   
Install Spacy Language Model

    python -m spacy download en_core_web_sm 

Usage
-----
    # Print usage to scan databases
    piicatcher db -h
    usage: piicatcher db [-h] -s HOST [-R PORT] [-u USER] [-p PASSWORD]
                     [-t {sqlite,mysql,postgres}] [-c {deep,shallow}]
                     [-o OUTPUT] [-f {ascii_table,json,orm}]

    optional arguments:
      -h, --help            show this help message and exit
      -s HOST, --host HOST  Hostname of the database. File path if it is SQLite
      -R PORT, --port PORT  Port of database.
      -u USER, --user USER  Username to connect database
      -p PASSWORD, --password PASSWORD
                            Password of the user
      -t {sqlite,mysql,postgres}, --connection-type {sqlite,mysql,postgres}
                            Type of database
      -c {deep,shallow}, --scan-type {deep,shallow}
                            Choose deep(scan data) or shallow(scan column names
                            only)
      -o OUTPUT, --output OUTPUT
                            File path for report. If not specified, then report is
                            printed to sys.stdout
      -f {ascii_table,json,orm}, --output-format {ascii_table,json,orm}
                            Choose output format type

    usage: piicatcher files [-h] [--path PATH] [--output OUTPUT]
                        [--output-format {ascii_table,json,orm}]


    piicatcher files -h
    # Print usage to scan databases
    optional arguments:
      -h, --help            show this help message and exit
      --path PATH           Path to file or directory
      --output OUTPUT       File path for report. If not specified, then report is
                            printed to sys.stdout
      --output-format {ascii_table,json,orm}
                            Choose output format type


Example
-------
     
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

