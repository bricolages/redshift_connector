require 'zlib'

module RedshiftConnector
  class AbstractDataFile
    def each_row(&block)
      f = if gzipped_object?
            Zlib::GzipReader.new(content)
          else
            content
          end
      @reader_class.new(f).each(&block)
    ensure
      content.close
    end

    def data_object?
      @reader_class.data_object?(key)
    end

    def gzipped_object?
      File.extname(key) == '.gz'
    end
  end
end
