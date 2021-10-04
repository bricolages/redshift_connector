require_relative 'helper'
require 'test/unit'
require 'redshift_connector'

class TestS3Import < Test::Unit::TestCase
  def test_import_delta_tsv
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_delta_from_s3(
      prefix: "#{$TEST_SCHEMA}_export/item_pvs_tsv/#{data_date}/item_pvs.tsv.",
      format: :tsv,

      table: 'item_pvs',
      columns: %w[id data_date item_id pv uu],
      upsert_columns: %w[pv uu]
    )
    job.execute
  end

  def test_import_all
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_all_from_s3(
      strategy: 'truncate',

      prefix: "#{$TEST_SCHEMA}_export/item_pvs_tsv/#{data_date}/item_pvs.tsv.",
      format: :tsv,

      table: 'item_pvs',
      columns: %w[id data_date item_id pv uu]
    )
    job.execute
  end
end
