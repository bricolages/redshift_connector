require 'redshift-connector/data_file_base'
require 'uri'
require 'zlib'
require 'open3'

module RedshiftConnector
  class UrlDataFile < DataFileBase
    def initialize(url, reader_class:)
      @url = url
      @reader_class = reader_class
    end

    def key
      URI.parse(@url).path
    end

    def content
      stdin, stdout, stderr, wait_th = Open3.popen3('curl', @url)
      stdin.close
      stdout
    end
  end
end
