require 'redshift-connector/s3_bucket'
require 'redshift-connector/s3_data_file'
require 'redshift-connector/logger'
require 'redshift-connector/data_file'
require 'aws-sdk'

module RedshiftConnector
  class S3DataFileBundle < AbstractDataFileBundle
    def self.for_prefix(bucket: S3Bucket.default, prefix:, format:, filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      real_prefix = "#{bucket.prefix}/#{prefix}"
      new(bucket, real_prefix, format: format, filter: filter, batch_size: batch_size, logger: logger)
    end

    def self.for_table(bucket: S3Bucket.default, schema:, table:, txn_id:, filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      prefix = "#{bucket.prefix}/#{schema}_export/#{table}/#{txn_id}/#{table}.csv."
      new(bucket, prefix, format: :redshift_csv, filter: filter, batch_size: batch_size, logger: logger)
    end

    def initialize(bucket, prefix, format: :csv, filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      @bucket = bucket
      @prefix = prefix
      @format = format
      @filter = filter || lambda {|*row| row }
      @batch_size = batch_size
      @logger = logger
      @reader_class = Reader.get(format)
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
        @logger.info "DELETE #{pref}*"
        @bucket.delete_objects(keys)
      end
    end
  end
end
