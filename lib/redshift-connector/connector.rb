require 'redshift-connector/exporter'
require 'redshift-connector/importer'
require 'redshift-connector/s3_data_file_bundle'
require 'redshift-connector/logger'

module RedshiftConnector
  class Connector
    def Connector.transport_delta(
        schema:,
        table: nil,
        src_table: table,
        dest_table: table,
        condition:,
        columns:,
        delete_cond: nil,
        upsert_columns: nil,
        bucket: nil,
        txn_id:, filter:,
        logger: RedshiftConnector.logger,
        quiet: false
    )
      unless src_table and dest_table
        raise ArgumentError, "missing :table, :src_table or :dest_table"
      end
      bucket = bucket ? S3Bucket.get(bucket) : S3Bucket.default
      logger = NullLogger.new if quiet
      bundle = S3DataFileBundle.for_table(
        bucket: bucket,
        schema: schema,
        table: src_table,
        txn_id: txn_id,
        filter: filter,
        logger: logger
      )
      exporter = Exporter.for_table_delta(
        bundle: bundle,
        schema: schema,
        table: src_table,
        columns: columns,
        condition: condition,
        logger: logger
      )
      importer = Importer.transport_delta_from_bundle(
        bundle: bundle,
        table: dest_table, columns: columns,
        delete_cond: delete_cond, upsert_columns: upsert_columns,
        logger: logger, quiet: quiet
      )
      new(exporter: exporter, importer: importer, logger: logger)
    end

    def Connector.transport_all(
        strategy: 'rename',
        schema:,
        table:,
        src_table: table,
        dest_table: table,
        columns:,
        bucket: nil,
        txn_id:,
        filter:,
        logger: RedshiftConnector.logger,
        quiet: false
    )
      bucket = bucket ? S3Bucket.get(bucket) : S3Bucket.default
      logger = NullLogger.new if quiet
      bundle = S3DataFileBundle.for_table(
        bucket: bucket,
        schema: schema,
        table: table,
        txn_id: txn_id,
        filter: filter,
        logger: logger
      )
      exporter = Exporter.for_table(
        bundle: bundle,
        schema: schema,
        table: table,
        columns: columns,
        logger: logger
      )
      importer = Importer.transport_all_from_bundle(
        strategy: strategy,
        bundle: bundle,
        table: table, columns: columns,
        logger: logger, quiet: quiet
      )
      new(exporter: exporter, importer: importer, logger: logger)
    end

    def initialize(exporter:, importer:, logger:)
      @exporter = exporter
      @importer = importer
      @logger = logger
    end

    def export_enabled?
      not ENV['IMPORT_ONLY']
    end

    def import_enabled?
      not ENV['EXPORT_ONLY']
    end

    def execute
      export if export_enabled?
      import if import_enabled?
    end

    def export
      @logger.info "==== export task =================================================="
      @exporter.execute
    end

    def import
      @logger.info "==== import task =================================================="
      @importer.execute
    end
  end
end
