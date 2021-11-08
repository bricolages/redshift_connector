require 'redshift_connector/query'
require 'redshift_connector/logger'

module RedshiftConnector
  class ExporterBuilder
    def initialize(ds:, exporter_class:)
      @ds = ds
      @exporter_class = exporter_class
    end

    def build_for_table_delta(schema:, table:, condition:, columns:, enable_cast:, bundle_params:, logger: RedshiftConnector.logger)
      query = DeltaQuery.new(schema: schema, table: table, columns: columns, condition: condition)
      @exporter_class.new(ds: @ds, query: query, bundle_params: bundle_params, enable_cast: enable_cast, logger: logger)
    end

    def build_for_table(schema:, table:, columns:, enable_cast:, bundle_params:, logger: RedshiftConnector.logger)
      query = SelectAllQuery.new(schema: schema, table: table, columns: columns)
      @exporter_class.new(ds: @ds, query: query, bundle_params: bundle_params, enable_cast: enable_cast, logger: logger)
    end

    def build_for_query(
        schema:,
        table:,
        bucket: nil,
        query:,
        query_params: [],
        txn_id: "#{Time.now.strftime('%Y%m%d_%H%M%S')}_#{$$}",
        enable_sort: false,
        enable_cast: false,
        logger: RedshiftConnector.logger,
        quiet: false
    )
      logger = NullLogger.new if quiet
      bundle_params = DataFileBundleParams.new(
        bucket: bucket,
        schema: schema,
        table: table,
        txn_id: txn_id,
        logger: logger
      )
      @exporter_class.new(
        ds: @ds,
        query: ArbitraryQuery.new(query),
        query_params: query_params,
        bundle_params: bundle_params,
        enable_sort: enable_sort,
        enable_cast: enable_cast,
        logger: logger
      )
    end

  end
end
