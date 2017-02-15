require 'redshift-connector/importer/activerecord-import'
require 'redshift-connector/logger'

module RedshiftConnector
  class Importer::Upsert
    def initialize(dao:, bundle:, columns:, upsert_columns:, logger: RedshiftConnector.logger)
      @dao = dao
      @bundle = bundle
      @columns = columns
      @upsert_columns = upsert_columns
      @logger = logger
    end

    def execute
      import
    end

    def import
      @logger.info "IMPORT #{@bundle.url}* -> #{@dao.table_name} (#{@columns.join(', ')}) upsert (#{@upsert_columns.join(', ')})"
      @bundle.each_batch do |rows|
        @dao.import(@columns, rows, on_duplicate_key_update: @upsert_columns)
      end
    end
  end
end
