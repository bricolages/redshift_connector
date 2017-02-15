require 'zlib'

module RedshiftConnector
  class S3DataFile
    def initialize(object, reader_class:)
      @object = object
      @reader_class = reader_class
    end

    def key
      @object.key
    end

    def each_row(&block)
      response = @object.get
      f = if gzipped_object?
        Zlib::GzipReader.new(response.body)
      else
        response.body
      end
      @reader_class.new(f).each(&block)
    ensure
      response.body.close if response
    end

    def data_object?
      @reader_class.data_object?(@object)
    end

    def gzipped_object?
      File.extname(@object.key) == '.gz'
    end
  end
end
