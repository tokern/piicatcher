[![Build Status](https://travis-ci.org/dblintio/piicatcher.svg?branch=master)](https://travis-ci.org/dblintio/piicatcher)
[![codecov](https://codecov.io/gh/dblintio/piicatcher/branch/master/graph/badge.svg)](https://codecov.io/gh/dblintio/piicatcher)

Pii Catcher for MySQL, PostgreSQL & AWS Redshift
================================================

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

PiiCatcher uses two types of scanners to detect PII information:
1. [CommonRegex](https://github.com/madisonmay/CommonRegex) uses a set of regular expressions 
for common types of information
2. [Spacy Named Entity Recognition](https://spacy.io/usage/linguistic-features#named-entities) 
uses Natural Language Processing to detect named entities. Only English language is currently supported.

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
    # Print usage
    $ piicatcher -h
    usage: piicatcher [-h] -s HOST [-u USER] [-p PASSWORD] [-t {sqlite,mysql}]
                      [-o OUTPUT] [-f {ascii_table}]

    optional arguments:
      -h, --help            show this help message and exit
      -s HOST, --host HOST  Hostname of the database. File path if it is SQLite
      -u USER, --user USER  Username to connect database
      -p PASSWORD, --password PASSWORD
                            Password of the user
      -t {sqlite,mysql}, --connection-type {sqlite,mysql}
                            Type of database
      -o OUTPUT, --output OUTPUT
                            File path for report. If not specified, then report is
                            printed to sys.stdout
      -f {ascii_table}, --output-format {ascii_table}
                            Choose output format type


Example
-------
     
    # run piicatcher on a sqlite db and print report to console
    piicatcher -c '/db/sqlqb'
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

