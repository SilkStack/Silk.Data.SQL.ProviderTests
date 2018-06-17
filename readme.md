# Silk.Data.SQL.ProviderTests

## Overview

A test suite for testing functionality of Silk.Data.SQL.Base providers.

## Requirements

Running these tests requires a the provider you wish to test a database configured to the following specifications:

* Database/schema must have no tables besides the following:
```sql
CREATE TABLE TableExistsTest (
  TestColumn int
);
```

## Features

* Designed to run on a build server, using docker images to provide the database to test against
* Designed to run in Visual Studio as you develop the provider...somehow

## Running

TBD