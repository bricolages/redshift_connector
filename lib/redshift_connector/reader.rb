# create module
module RedshiftConnector
  module Reader
  end
end

require 'redshift_connector/reader/redshift_csv'
require 'redshift_connector/reader/csv'
require 'redshift_connector/reader/tsv'
require 'redshift_connector/reader/exception'

module RedshiftConnector
  module Reader
    def Reader.get(id)
      Abstract.get_reader_class(id)
    end
  end
end
