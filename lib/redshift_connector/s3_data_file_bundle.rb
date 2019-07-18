require 'redshift_connector/abstract_data_file_bundle'
require 'redshift_connector/s3_bucket'
require 'redshift_connector/s3_data_file'
require 'redshift_connector/logger'
require 'aws-sdk-s3'

module RedshiftConnector
  class S3DataFileBundle < AbstractDataFileBundle
    def self.for_params(params)
      unless params.txn_id
        raise ArgumentError, "cannot create bundle: missing txn_id"
      end
      s3bucket = params.bucket ? S3Bucket.get(params.bucket) : S3Bucket.default
      for_table(
        bucket: s3bucket,
        schema: params.schema,
        table: params.table,
        txn_id: params.txn_id,
        filter: params.filter,
        logger: params.logger
      )
    end

    def self.for_prefix(bucket: S3Bucket.default, prefix:, format:, filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      real_prefix = "#{bucket.prefix}/#{prefix}"
      new(bucket, real_prefix, format: format, filter: filter, batch_size: batch_size, logger: logger)
    end

    def self.for_table(bucket: S3Bucket.default, schema:, table:, txn_id:, filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      prefix = "#{bucket.prefix}/#{schema}_export/#{table}/#{txn_id}/#{table}.csv."
      new(bucket, prefix, format: :redshift_csv, filter: filter, batch_size: batch_size, logger: logger)
    end

    def initialize(bucket, prefix, format: :csv, filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      super filter: filter, batch_size: batch_size, logger: logger
      @bucket = bucket
      @prefix = prefix
      @format = format
      @reader_class = Reader.get(format)
      logger.info "reader: #{@reader_class}"
    end

    attr_reader :bucket
    attr_reader :prefix

    def url
      "s3://#{@bucket.name}/#{@prefix}"
    end

    def credential_string
      @bucket.credential_string
    end

    def data_files
      @bucket.objects(prefix: @prefix)
        .map {|obj| S3DataFile.new(obj, reader_class: @reader_class) }
    end

    def clear
      pref = File.dirname(@prefix) + '/'
      keys = @bucket.objects(prefix: pref).map(&:key)
      unless keys.empty?
        logger.info "DELETE #{pref}*"
        @bucket.delete_objects(keys)
      end
    end
  end
end
