# Release Note

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
