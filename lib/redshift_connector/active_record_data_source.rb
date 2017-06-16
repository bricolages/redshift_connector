require 'redshift_connector/exporter_builder'

module RedshiftConnector
  class ActiveRecordDataSource
    def ActiveRecordDataSource.for_dao(dao)
      new(dao)
    end

    def initialize(dao)
      @dao = dao
    end

    def exporter_builder
      ExporterBuilder.new(ds: self, exporter_class: ActiveRecordExporter)
    end

    def execute_query(query_str)
      @dao.connection_pool.with_connection {|conn|
        conn.execute(query_str)
      }
    end
  end
end
