require 'redshift_connector/logger'

module RedshiftConnector
  class DataFileBundleParams
    def initialize(
      bucket: nil,
      schema:,
      table:,
      txn_id: nil,
      filter:,
      logger: RedshiftConnector.logger
    )
      @bucket = bucket
      @schema = schema
      @table = table
      @txn_id = txn_id
      @filter = filter
      @logger = logger
    end

    attr_reader :bucket
    attr_reader :schema
    attr_reader :table
    attr_reader :txn_id
    attr_reader :filter
    attr_reader :logger
  end
end
