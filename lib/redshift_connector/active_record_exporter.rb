require 'redshift_connector/s3_data_file_bundle'
require 'redshift_connector/query'
require 'redshift_connector/logger'

module RedshiftConnector
  class ActiveRecordExporter
    def initialize(ds:, query:, bundle_params:, enable_sort: false, enable_cast: false, logger: RedshiftConnector.logger)
      raise ArgumentError, "ActiveRecordExporter does not support type cast" if enable_cast
      @ds = ds
      @query = query
      @bundle_params = bundle_params
      @enable_sort = enable_sort
      @logger = logger

      @bundle = S3DataFileBundle.for_params(bundle_params)
    end

    attr_reader :query
    attr_reader :bundle_params
    attr_reader :bundle
    attr_reader :logger

    def execute
      @bundle.clear
      unload_query = UnloadQuery.new(query: @query, bundle: @bundle, enable_sort: @enable_sort)
      @logger.info "EXPORT #{unload_query.description} -> #{@bundle.url}*"
      stmt = unload_query.to_sql
      @logger.info "[SQL/Redshift] #{batch_job_label}#{stmt.strip}"
      @ds.execute_query(batch_job_label + stmt)
      @bundle
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
        batch_file = caller.detect {|c| /redshift_connector|active_record/ !~ c }
        path = batch_file ? batch_file.split(':').first : '?'
        "/* Job: #{app}:#{path} */ "
      end
    end
  end
end
