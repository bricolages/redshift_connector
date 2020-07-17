# Release Note

## version 8.0.1

- [fix] Loosen pg version restriction to allow >0.18 to support Rails 5.
  Note that Redshift is PostgreSQL 8 compatible and pg >0.18 does not support PostgreSQL 8.
  Now you can use any version of pg and most version of pg works with Redshift, but you must take a risk.

## version 8.0.0

- [INCOMPATIBLE] This library is renamed to "redshift_connector".  Just modify your Gemfile from "redshift-connector" to "redshift_connector".
- [INCOMPATIBLE] redshift-connector-data_file gem is merged.
- [INCOMPATIBLE] (internal) *DataFileBundle#each, #each_row, #each_object, #each_batch, #all_data_objects are removed.  Use DataFileBundleReader class instead.
- [INCOMPATIBLE] (internal) AbstractDataFileBundle class is removed.
- [INCOMPATIBLE] (internal) AbstractDataFile class is removed.

## version 7.2.2

- [fix] RedshiftConnector.transport_all: src_table/dest_table parameter did not work.
- [fix] RedshiftConnector.transport_all (strategy=rename): newer activerecord-import requires class name.

## version 7.2.1

- no change.

## version 7.2.0

- Removes aws-sdk dependency

## version 7.0.2

- [fix] RedshiftConnector.foreach did not work

## version 7.0.1

- [fix] RedshiftConnector.transport_delta_from_s3, .transport_all_from_s3 were wrongly dropped, restore them.

## version 7.0.0

- [INCOMPATIBLE] Library hierarchy changed: redshift-connector/* -> redshift_connector/*.  redshift-connector.rb still exists as an entry point for bundler.
- [new] Exporter becomes pluggable.  You can implement your own exporter data source instead of ActiveRecord.

## version 6.0.0

- version number change only.

## version 5.6.0

- Unifies version 4.x (supports Rails 4) and 5.x (supports Rails 5).

## version 4.5.0 / 5.5.0

- [new] Separates S3 access layer to another gem: redshift-connector-data_file

## version 4.4.1 / 5.4.1

- [new] New option enable_sort for Connector.foreach, to enforce global sorting.

## version 4.4.0 / 5.4.0

- [CHANGE] Drops export-only-once feature (and FORCE environment switch), it is not so useful.
  Exporter now always exports data.

## version 4.3.2 / 5.3.2

- [new] Allows reading from S3 signed URL (for separated export/import processes)

## version 4.3.1 / 5.3.1

- First release for Rails 5 series.
- [fix] Add option for AWS multi-regions support

## version 4.3.0

- [new] New method RedshiftConnector.foreach to read rows with UNLOAD

## version 4.2.0

- [new] New methods RedshiftConnector.transport_delta_from_s3, .transport_all_from_s3 to read from S3
 
## version 4.1.0

- [new] Introduces rebuild operator.  New facade method Connector.transport_all.
  
## version 4.0.2
  
- [fix] Correctly parses UNLOAD-generated CSV (dangerous characeters are escaped by backslash).
  
## version 4.0.1
  
- [new] Allow configure the default logger by RedshiftConnector.logger=.
  
## version 4.0.0
  
First release for Rails 4 series.
