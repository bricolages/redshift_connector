# Release Note

## version 4.4.0

- [CHANGE] Drops export-only-once feature (and FORCE environment switch), it is not so useful.
  Exporter now always exports data.

## version 4.3.2

- [new] Allows reading from S3 signed URL (for separated export/import processes)

## version 4.3.1

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
