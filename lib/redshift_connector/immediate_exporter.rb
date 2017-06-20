require 'redshift_connector/s3_data_file_bundle'
require 'redshift_connector/logger'

module RedshiftConnector
  class ImmediateExporter
    def initialize(bundle:, logger: RedshiftConnector.logger)
      @bundle = bundle
      @logger = logger
    end

    attr_reader :bundle
    attr_reader :logger

    def execute
      @logger.info "USE #{@bundle.url}*"
      @bundle
    end
  end
end
