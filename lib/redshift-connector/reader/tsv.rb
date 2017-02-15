require 'redshift-connector/reader/abstract'
require 'redshift-connector/reader/exception'
require 'csv'

module RedshiftConnector
  # Parses TSV (Tab Separated Format) files.
  class Reader::TSV < Reader::Abstract
    declare_reader :tsv

    def self.data_object?(obj)
      /\.tsv(?:\.|\z)/ =~ File.basename(obj.key)
    end

    def initialize(f)
      @f = f
    end

    def each(&block)
      @f.each_line do |line|
        yield line.chomp.split("\t", -1)
      end
    end
  end
end
