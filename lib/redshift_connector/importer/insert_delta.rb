require 'redshift_connector/importer/activerecord-import'
require 'redshift_connector/logger'

module RedshiftConnector
  class Importer::InsertDelta
    def initialize(dao:, columns:, delete_cond:, logger: RedshiftConnector.logger)
      @dao = dao
      @columns = columns
      @delete_cond = delete_cond
      @logger = logger
    end

    def execute(bundle)
      delete_rows(@delete_cond)
      import(bundle)
    end

    def delete_rows(cond_expr)
      @logger.info "DELETE #{@dao.table_name} where (#{cond_expr})"
      @dao.connection.execute("delete from #{@dao.table_name} where #{cond_expr}")
      @logger.info "deleted."
    end

    def import(bundle)
      @logger.info "IMPORT #{bundle.url}* -> #{@dao.table_name} (#{@columns.join(', ')})"
      bundle.each_batch do |rows|
        @dao.import(@columns, rows)
      end
    end
  end
end
