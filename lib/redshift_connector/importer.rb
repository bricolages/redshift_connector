# create module
module RedshiftConnector
  module Importer
  end
end

require 'redshift_connector/importer/upsert'
require 'redshift_connector/importer/insert_delta'
require 'redshift_connector/importer/rebuild_rename'
require 'redshift_connector/importer/rebuild_truncate'
require 'redshift_connector/logger'

module RedshiftConnector
  module Importer
    def Importer.for_delta_upsert(table:, columns:, delete_cond: nil, upsert_columns: nil, logger: RedshiftConnector.logger)
      if delete_cond and upsert_columns
        raise ArgumentError, "delete_cond and upsert_columns are exclusive"
      end
      importer =
        if delete_cond
          Importer::InsertDelta.new(
            dao: table.classify.constantize,
            columns: columns,
            delete_cond: delete_cond,
            logger: logger
          )
        elsif upsert_columns
          Importer::Upsert.new(
            dao: table.classify.constantize,
            columns: columns,
            upsert_columns: upsert_columns,
            logger: logger
          )
        else
          raise ArgumentError, "either of delete_cond or upsert_columns is required for delta import"
        end
      importer
    end

    def Importer.for_rebuild(strategy: 'rename', table:, columns:, logger: RedshiftConnector.logger)
      c = get_rebuild_class(strategy)
      c.new(
        dao: table.classify.constantize,
        columns: columns,
        logger: logger
      )
    end

    def Importer.get_rebuild_class(strategy)
      case strategy.to_s
      when 'rename' then RebuildRename
      when 'truncate' then RebuildTruncate
      else
        raise ArgumentError, "unsupported rebuild strategy: #{strategy.inspect}"
      end
    end
  end
end
