require 'redshift_connector/importer/activerecord-import'
require 'redshift_connector/logger'

module RedshiftConnector
  class Importer::RebuildRename
    def initialize(dao:, columns:, logger: RedshiftConnector.logger)
      @dao = dao
      @columns = columns
      @logger = logger
    end

    def execute(bundle)
      dest_table = @dao.table_name
      tmp_table = "#{dest_table}_new"
      old_table = "#{dest_table}_old"

      tmp_dao = @dao.dup
      tmp_dao.table_name = tmp_table

      exec_update "drop table if exists #{tmp_table}"
      exec_update "create table #{tmp_table} like #{dest_table}"
      import(tmp_dao, bundle)
      exec_update "drop table if exists #{old_table}"
      # Atomic table exchange
      exec_update "rename table #{dest_table} to #{old_table}, #{tmp_table} to #{dest_table}"
    end

    def exec_update(query)
      @logger.info query
      @dao.connection.execute(query)
    end

    def import(dao, bundle)
      @logger.info "IMPORT #{bundle.url}* -> #{dao.table_name} (#{@columns.join(', ')})"
      bundle.each_batch do |rows|
        dao.import(@columns, rows)
      end
    end
  end
end
