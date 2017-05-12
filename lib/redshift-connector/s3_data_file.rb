require 'redshift-connector/data_file'

module RedshiftConnector
  class S3DataFile < AbstractDataFile
    def initialize(object, reader_class:)
      @object = object
      @reader_class = reader_class
    end

    def key
      @object.key
    end

    def content
      @object.get.body
    end

    delegate :presigned_url, to: :@object
  end
end
