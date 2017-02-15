require 'redshift-connector/importer/activerecord-import'
require 'redshift-connector/logger'

module RedshiftConnector
  class Importer::RebuildTruncate
    def initialize(dao:, bundle:, columns:, logger: RedshiftConnector.logger)
      @dao = dao
      @bundle = bundle
      @columns = columns
      @logger = logger
    end

    def execute
      truncate_table(@dao.table_name)
      import
    end

    def truncate_table(table_name)
      @logger.info "TRUNCATE #{table_name}"
      @dao.connection.execute("truncate #{table_name}")
      @logger.info "truncated."
    end

    def import
      @logger.info "IMPORT #{@bundle.url}* -> #{@dao.table_name} (#{@columns.join(', ')})"
      @bundle.each_batch do |rows|
        @dao.import(@columns, rows)
      end
    end
  end
end
