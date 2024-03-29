# Python Challenge

Requires Python 3.7

Install dependencies with `pip install -r requirements.txt`, then run with `python -m challenge.runner <path-to-ip-file>`. This will parse the IP file, query data for each IP, and save that data to disk. Then, it will go into an input loop allowing you to search the data. Type `exit`, `quit`, or enter `CTRL+C` to quit. Type `search <query-string>` to search the data.

If you want to search the existing data without loading a new IP file, run it without a path: `python -m challenge.runner`.

Throughout the rest of this document, I will use the terms "search" and "query" interchangebly.

Before going into searching, I will explain the concept of the index.

## The Index

An index is a group of like-data; it can be thought of as a SQL table. All events belong to an index. There are three indices:

1. *geoip*: contains GeoIP data
2. *rdap*: contains RDAP data
3. *ip_rdap*: a joiner-table of sorts, existing to join an IP with RDAP data (IPs are not stored with RDAP data as RDAP data can apply to multiple IPs)

Each index has its own file. For example, the file for the `geoip` index is `data/geoip.json`.

## Searching

Searching is inspired by Splunk's query language.

A query can consist of multiple commands separated by a pipe (`|`). Imagine a multiple-command search as a "pipeline" where each command is applied to the data in turn, with the data being fed from one command to the next until the end of the pipeline.

There are four commands available:

### 1. search

The `search` command allows you to filter the data using key-value pairs and modifiers like `OR` and `NOT`. It must be the first command in the query.

#### Usage:

search \<expression>...

**\<expression>**

\<comparison-expression> | NOT \<expression> | \<expression> OR \<expression>

**\<comparison-expression>**

\<field>\<operator>\<value>

**\<operator>**

= | != | < | <= | > | >=

#### Examples

##### Retrieving data from an index

This search will return all data from the `geoip` index.

`search index=geoip`

##### Retrieving GeoIP data for specific IPs

Use the `OR` modifier to specify multiple values for a field.

`search index=geoip ip=192.168.1.10 OR ip=192.168.1.11`

##### Retrieving GeoIP data for all IPs except one

`search index=geoip ip!=192.168.1.15`

or

`search index=geoip NOT ip=192.268.1.15`

#### Retrieving data for a specific IP from multiple indices

It is not required to search by index.

`search ip=192.168.1.15`

The above search will return data with `ip=192.168.1.15` from all indices (in this case, data from indices `geoip` and `ip_rdap` will be returned; events in `rdap` do not contain an `ip` field).

### 2. fields

The `fields` command allows you to display only the fields you want to see.

#### Usage

fields \<field>...

#### Example

Remove all fields from the results except for `ip` and `continent_name`:

`search index=geoip | fields ip continent_name`

### 3. join

The `join` command allows you to join data together by a field (the "by-field"). Each event that shares the same value for the by-field is joined together under one event. This allows you to join data from two disparate data sources.

#### Usage

join BY \<by-field>

#### Example

Join an IP with its associated RDAP data using the `ip_rdap` and `rdap` indices:

`search index=ip_rdap OR index=rdap | join BY handle`

`handle` is the 'by-field', the field that is shared by the different kinds of data.

### 4. prettyprint

The `prettyprint` command may only be used as the last command in the search. It allows you to print the result set in a prettier fashion than plain JSON blobs.

#### Usage

prettyprint format=\<format>

**\<format>**

json | table

#### Examples

##### Print results as pretty JSON

Using `format=json` still prints each result as JSON but with newlines and indentation.

`search index=rdap | prettyprint format=json`

##### Print results as a table

Using `format=table` prints the results as a formatted table.

`search index=rdap | prettyprint format=table`

If there are a lot of fields in the result set, the results will overflow onto the next line(s); therefore, it is recommended to pare down unwanted fields using `fields` before using `prettyprint format=table`. This happens expecially when joining `ip_rdap` and `rdap` data together. Many IPs share the same `rdap` data, so the IP values will become very long. I recommend specifying the IP(s) you are interested in before doing the `join`.
