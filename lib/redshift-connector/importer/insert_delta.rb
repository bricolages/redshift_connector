require 'redshift-connector/importer/activerecord-import'
require 'redshift-connector/logger'

module RedshiftConnector
  class Importer::InsertDelta
    def initialize(dao:, bundle:, columns:, delete_cond:, logger: RedshiftConnector.logger)
      @dao = dao
      @bundle = bundle
      @columns = columns
      @delete_cond = delete_cond
      @logger = logger
    end

    def execute
      delete_rows(@delete_cond)
      import
    end

    def delete_rows(cond_expr)
      @logger.info "DELETE #{@dao.table_name} where (#{cond_expr})"
      @dao.connection.execute("delete from #{@dao.table_name} where #{cond_expr}")
      @logger.info "deleted."
    end

    def import
      @logger.info "IMPORT #{@bundle.url}* -> #{@dao.table_name} (#{@columns.join(', ')})"
      @bundle.each_batch do |rows|
        @dao.import(@columns, rows)
      end
    end
  end
end
