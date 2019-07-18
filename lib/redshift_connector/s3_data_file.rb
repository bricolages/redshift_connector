require 'redshift_connector/data_file'

module RedshiftConnector
  class S3DataFile < AbstractDataFile
    def initialize(object, reader_class:)
      super reader_class: reader_class
      @object = object
    end

    def key
      @object.key
    end

    def open
      @object.get.body
    end

    delegate :presigned_url, to: :@object
  end
end
