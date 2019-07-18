require 'zlib'

module RedshiftConnector
  class AbstractDataFile
    def initialize(reader_class:)
      @reader_class = reader_class
    end

    def each_row(&block)
      f = open
      begin
        if gzipped_object?
          f = Zlib::GzipReader.new(f)
        end
        @reader_class.new(f).each(&block)
      ensure
        f.close
      end
    end

    # abstract open

    def data_object?
      @reader_class.data_object?(key)
    end

    def gzipped_object?
      File.extname(key) == '.gz'
    end
  end
end
