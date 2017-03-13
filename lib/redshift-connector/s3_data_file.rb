require 'redshift-connector/data_file_base'

module RedshiftConnector
  class S3DataFile < DataFileBase
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
