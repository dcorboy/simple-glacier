# Simple Glacier

A command-line utility to easily manage collections of archive files on Amazon Glacier

## Motivation

I wanted a cheap and easy way to archive data in case of a catastrophic loss of my home server. [Amazon S3](https://aws.amazon.com/s3/) is great for this, but the cost of storing my 1TB gzipped tar file was surprisingly expensive.

[Amazon Glacier](https://aws.amazon.com/glacier/) seemed like a perfect alternative at less than a quarter of the cost of S3 but both the multi-part upload of large files as well as the management of many split files was very difficult when using the [AWS Command Line Interface](https://aws.amazon.com/cli/).

This simple command-line utility, developed using the [AWS SDK for Ruby](http://docs.aws.amazon.com/sdkforruby/api/index.html), simplifies the task of uploading, managing and removing archives as a named collection.

## Installation

You'll need to have an Amazon AWS account and have your AWS Access Key ID, AWS Secret Access Key and Default region name configured. Simple-Glacier will use these credentials directly.

If you already have Ruby and the AWS SDK installed, you can simply download the `simple_glacier.rb' file and get started.

## Usage

```
Usage: simple_glacier.rb [options] command [files]
    -n, --name_upload=NAME           Name the upload collection for later reference
    -r, --receipts_file=FILE         JSON archive of upload-receipts
    -v, --vault_name=VAULT           Glacier vault name
    -d, --dry_run                    If flag is present, no actions are taken and are instead displayed
    -h, --help                       Prints this help

Commmands:
    upload file [file..]     --  Upload files as a named collection, appending to an existing collection
      list                   --  List file information for a named collection, or list all collections
    delete                   --  Delete all Glacier archive files in named collection
```

Simple-Glacier stores the metadata for every archive in a local JSON file (default: ./glacier_receipts.json). This allows you to list your files by the upload collection name you designate (a random name will be generated if one is not supplied.

## License

[GNU General Public License](http://www.gnu.org/licenses/)

&copy; Copyright 2015 Dave Corboy <dave@corboy.com>

This file is part of Simple-Glacier.

Simple-Glacier is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Simple-Glacier is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

If you did not receive a copy of the GNU General Public License
along with Simple-Glacier, see <http://www.gnu.org/licenses/>.