require 'redshift_connector/logger'
require 'forwardable'

module RedshiftConnector
  class DataFileBundleReader
    extend Forwardable

    DEFAULT_BATCH_SIZE = 1000

    def initialize(bundle, filter: nil, batch_size: DEFAULT_BATCH_SIZE, logger: RedshiftConnector.logger)
      @bundle = bundle
      @filter = filter || lambda {|*row| row }
      @batch_size = batch_size || 1000
      @logger = logger
    end

    attr_reader :bundle
    attr_reader :batch_size
    attr_reader :logger

    def_delegators '@bundle', :url, :bucket, :key

    def each_row(&block)
      each_object do |obj|
        if @bundle.has_manifest?
          obj.each_row do |row|
            yield type_cast(row)
          end
        else
          obj.each_row(&block)
        end

      end
    end

    alias each each_row

    def each_object(&block)
      all_data_objects.each do |obj|
        @logger.info "processing s3 object: #{obj.key}"
        yield obj
      end
    end

    def all_data_objects
      @bundle.data_files.select {|obj| obj.data_object? }
    end

    REPORT_SIZE = 10_0000

    def each_batch(report: true)
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

    def type_cast(row)
      row.zip(@bundle.manifest_file.column_types).map do |value, type|
        next nil if (value == '' and type != 'character varing') # null becomes '' on unload

        case type
        when 'smallint', 'integer', 'bigint'
          value.to_i
        when 'numeric', 'double precision'
          value.to_f
        when 'character', 'character varying'
          value
        when 'timestamp without time zone', 'timestamp with time zone'
          Time.parse(value)
        when 'date'
          Date.parse(value)
        when 'boolean'
          value == 'true' ? true : false
        else
          raise "not support data type: #{type}"
        end
      end
    end
    private :type_cast
  end
end
