require 'redshift-connector/s3_bucket'
require 'redshift-connector/s3_data_file'
require 'redshift-connector/reader'
require 'redshift-connector/logger'
require 'redshift-connector/abstract_data_file_bundle'
require 'aws-sdk'

module RedshiftConnector
  class S3DataFileBundle < DataFileBundleBase
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

    REPORT_SIZE = 10_0000

    def each_batch(report: true)
      @logger.info "reader: #{@reader_class}"
      n = 0
      reported = 0
      do_each_batch(@batch_size) do |rows|
        yield rows
        n += rows.size
        if n / REPORT_SIZE > reported
          @logger.info "#{n} rows processed" if report
          reported = n / REPORT_SIZE
        end
      end
      @logger.info "total #{n} rows processed" if report
    end

    def do_each_batch(batch_size)
      filter = @filter
      buf = []
      each_row do |row|
        buf.push filter.(*row)
        if buf.size == batch_size
          yield buf
          buf = []
        end
      end
      yield buf unless buf.empty?
    end
    private :do_each_batch

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
