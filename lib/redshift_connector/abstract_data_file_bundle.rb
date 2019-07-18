module RedshiftConnector
  class AbstractDataFileBundle
    def initialize(filter: nil, batch_size: 1000, logger: RedshiftConnector.logger)
      @filter = filter || lambda {|*row| row }
      @batch_size = batch_size || 1000
      @logger = logger
    end

    attr_reader :batch_size
    attr_reader :logger

    def each_row(&block)
      each_object do |obj|
        obj.each_row(&block)
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
      data_files.select {|obj| obj.data_object? }
    end

    # abstract data_files

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
  end
end
