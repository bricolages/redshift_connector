require 'redshift_connector/exporter'
require 'redshift_connector/importer'
require 'redshift_connector/data_file_bundle_params'
require 'redshift_connector/logger'

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
        txn_id: nil,
        filter:,
        logger: RedshiftConnector.logger,
        quiet: false
    )
      unless src_table and dest_table
        raise ArgumentError, "missing :table, :src_table or :dest_table"
      end
      logger = NullLogger.new if quiet
      bundle_params = DataFileBundleParams.new(
        bucket: bucket,
        schema: schema,
        table: src_table,
        txn_id: txn_id,
        filter: filter,
        logger: logger
      )
      exporter = Exporter.for_table_delta(
        bundle_params: bundle_params,
        schema: schema,
        table: src_table,
        columns: columns,
        condition: condition,
        logger: logger
      )
      importer = Importer.for_delta_upsert(
        table: dest_table,
        columns: columns,
        delete_cond: delete_cond,
        upsert_columns: upsert_columns,
        logger: logger
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
        txn_id: nil,
        filter:,
        logger: RedshiftConnector.logger,
        quiet: false
    )
      logger = NullLogger.new if quiet
      bundle_params = DataFileBundleParams.new(
        bucket: bucket,
        schema: schema,
        table: src_table,
        txn_id: txn_id,
        filter: filter,
        logger: logger
      )
      exporter = Exporter.for_table(
        bundle_params: bundle_params,
        schema: schema,
        table: src_table,
        columns: columns,
        logger: logger
      )
      importer = Importer.for_rebuild(
        strategy: strategy,
        table: dest_table,
        columns: columns,
        logger: logger
      )
      new(exporter: exporter, importer: importer, logger: logger)
    end

    def initialize(exporter:, importer:, logger:)
      @exporter = exporter
      @importer = importer
      @logger = logger
      @bundle = nil
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
      @bundle = @exporter.execute
    end

    def import
      @logger.info "==== import task =================================================="
      @importer.execute(@bundle)
    end
  end
end
