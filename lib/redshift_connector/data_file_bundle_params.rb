require 'redshift_connector/logger'

module RedshiftConnector
  class DataFileBundleParams
    def initialize(
      bucket: nil,
      schema:,
      table:,
      txn_id: nil,
      logger: RedshiftConnector.logger
    )
      @bucket = bucket
      @schema = schema
      @table = table
      @txn_id = txn_id
      @logger = logger
    end

    attr_reader :bucket
    attr_reader :schema
    attr_reader :table
    attr_reader :txn_id
    attr_reader :logger
  end
end
