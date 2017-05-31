# create module
module RedshiftConnector
  module Importer
  end
end

require 'redshift-connector/importer/upsert'
require 'redshift-connector/importer/insert_delta'
require 'redshift-connector/importer/rebuild_rename'
require 'redshift-connector/importer/rebuild_truncate'

require 'redshift-connector/s3_data_file_bundle'
require 'redshift-connector/logger'

module RedshiftConnector
  module Importer
    def Importer.transport_delta_from_s3(
        bucket: nil, prefix:, format:, filter: nil,
        table:, columns:,
        delete_cond: nil, upsert_columns: nil,
        logger: RedshiftConnector.logger, quiet: false)
      bucket = bucket ? S3Bucket.get(bucket) : S3Bucket.default
      logger = NullLogger.new if quiet
      bundle = S3DataFileBundle.for_prefix(
        bucket: bucket,
        prefix: prefix,
        format: format,
        filter: filter,
        logger: logger
      )
      transport_delta_from_bundle(
        bundle: bundle,
        table: table, columns: columns,
        delete_cond: delete_cond, upsert_columns: upsert_columns,
        logger: logger, quiet: quiet
      )
    end

    def Importer.transport_delta_from_bundle(
      bundle:,
      table:, columns:,
      delete_cond: nil, upsert_columns: nil,
      logger: RedshiftConnector.logger, quiet: false
    )
      if delete_cond and upsert_columns
        raise ArgumentError, "delete_cond and upsert_columns are exclusive"
      end
      importer =
        if delete_cond
          Importer::InsertDelta.new(
            dao: table.classify.constantize,
            bundle: bundle,
            columns: columns,
            delete_cond: delete_cond,
            logger: logger
          )
        elsif upsert_columns
          Importer::Upsert.new(
            dao: table.classify.constantize,
            bundle: bundle,
            columns: columns,
            upsert_columns: upsert_columns,
            logger: logger
          )
        else
          raise ArgumentError, "either of delete_cond or upsert_columns is required for transport_delta"
        end
      importer
    end

    def Importer.transport_all_from_s3(
        strategy: 'rename',
        bucket: nil, prefix:, format:, filter: nil,
        table:, columns:,
        logger: RedshiftConnector.logger, quiet: false)
      bucket = bucket ? S3Bucket.get(bucket) : S3Bucket.default
      logger = NullLogger.new if quiet
      bundle = S3DataFileBundle.for_prefix(
        bucket: bucket,
        prefix: prefix,
        format: format,
        filter: filter,
        logger: logger
      )
      transport_all_from_bundle(
        strategy: strategy,
        bundle: bundle,
        table: table, columns: columns,
        logger: logger, quiet: quiet
      )
    end

    def Importer.transport_all_from_bundle(
      strategy: 'rename',
      bundle:,
      table:, columns:,
      logger: RedshiftConnector.logger, quiet: false
    )
      importer = get_rebuild_class(strategy).new(
        dao: table.classify.constantize,
        bundle: bundle,
        columns: columns,
        logger: logger
      )
      importer
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
