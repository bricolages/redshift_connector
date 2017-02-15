require 'redshift-connector/query'
require 'redshift-connector/logger'

module RedshiftConnector
  class Exporter
    def Exporter.default_data_source=(ds)
      @default_data_source = ds
    end

    def Exporter.default_data_source
      @default_data_source or raise ArgumentError, "RedshiftConnector::Exporter.default_data_source was not set"
    end

    def Exporter.for_table_delta(ds: default_data_source, schema:, table:, condition:, columns:, bundle:, logger: RedshiftConnector.logger)
      delta_query = DeltaQuery.new(schema: schema, table: table, columns: columns, condition: condition)
      unload_query = UnloadQuery.new(query: delta_query, bundle: bundle)
      new(ds: ds, query: unload_query, bundle: bundle, logger: logger)
    end

    def Exporter.for_table(ds: default_data_source, schema:, table:, columns:, bundle:, logger: RedshiftConnector.logger)
      query = SelectAllQuery.new(schema: schema, table: table, columns: columns)
      unload_query = UnloadQuery.new(query: query, bundle: bundle)
      new(ds: ds, query: unload_query, bundle: bundle, logger: logger)
    end

    def Exporter.foreach(**params, &block)
      exporter = Exporter.for_query(**params)
      begin
        exporter.execute
        exporter.bundle.each_row(&block)
      ensure
        exporter.bundle.clear
      end
    end

    def Exporter.for_query(
        ds: default_data_source,
        schema:,
        table:,
        bucket: nil,
        query:,
        txn_id: "#{Time.now.strftime('%Y%m%d_%H%M%S')}_#{$$}",
        filter: nil,
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
      exporter = Exporter.new(
        ds: ds,
        query: UnloadQuery.wrap(query: query, bundle: bundle),
        bundle: bundle,
        logger: logger
      )
      exporter
    end

    def initialize(ds: self.class.default_data_source, query:, bundle:, logger: RedshiftConnector.logger)
      @ds = ds
      @query = query
      @bundle = bundle
      @logger = logger
    end

    attr_reader :query
    attr_reader :bundle
    attr_reader :logger

    def completed?
      @bundle.bucket.object(flag_object_key).exists?
    end

    def create_flag_object
      @logger.info "TOUCH #{flag_object_key}"
      @bundle.bucket.object(flag_object_key).put(body: "OK")
    end

    def flag_object_key
      "#{File.dirname(@bundle.prefix)}/00completed"
    end

    def execute
      @bundle.clear
      @logger.info "EXPORT #{@query.description} -> #{@bundle.url}*"
      @ds.connection_pool.with_connection do |conn|
        stmt = @query.to_sql
        @logger.info "[SQL/Redshift] #{batch_job_label}#{stmt.strip}"
        conn.execute(batch_job_label + stmt)
      end
      create_flag_object
    end

    def batch_job_label
      @batch_job_label ||= begin
        components = Dir.getwd.split('/')
        app = if components.last == 'current'
            # is Capistrano environment
            components[-2]
          else
            components[-1]
          end
        batch_file = caller.detect {|c| /redshift-connector|active_record/ !~ c }
        path = batch_file ? batch_file.split(':').first : '?'
        "/* Job: #{app}:#{path} */ "
      end
    end
  end
end
