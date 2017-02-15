# create module
module RedshiftConnector
  module Reader
  end
end

require 'redshift-connector/reader/redshift_csv'
require 'redshift-connector/reader/csv'
require 'redshift-connector/reader/tsv'
require 'redshift-connector/reader/exception'

module RedshiftConnector
  module Reader
    def Reader.get(id)
      Abstract.get_reader_class(id)
    end
  end
end
