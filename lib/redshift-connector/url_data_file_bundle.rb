require 'redshift-connector/reader'
require 'redshift-connector/logger'
require 'redshift-connector/abstract_data_file_bundle'
require 'redshift-connector/url_data_file'

module RedshiftConnector
  class UrlDataFileBundle < DataFileBundleBase
    def initialize(data_file_urls, format: :redshift_csv, filter: nil, logger: RedshiftConnector.logger)
      @data_file_urls = data_file_urls
      @filter = filter || lambda {|*row| row }
      @logger = logger
      @reader_class = Reader.get(format)
    end

    def data_files
      @data_file_urls.map do |url|
        UrlDataFile.new(url, reader_class: @reader_class)
      end
    end
  end
end
