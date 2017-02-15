require_relative 'helper'
require 'test/unit'

class TestConnector < Test::Unit::TestCase
  def test_connector_upsert
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_delta(
      schema: $TEST_SCHEMA,
      table: 'item_pvs',

      txn_id: data_date,
      condition: %Q(data_date = date '#{data_date}'),

      columns: %w[id data_date item_id pv uu],
      upsert_columns: %w[pv uu],
      filter: -> (id, data_date, item_id, pv, uu) {
        [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
      }
    )
    job.execute
  end

  def test_connector_delete_insert
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_delta(
      schema: $TEST_SCHEMA,
      table: 'item_pvs',

      txn_id: data_date,
      condition: %Q(data_date = date '#{data_date}'),
      delete_cond: %Q(data_date = date '#{data_date}'),

      columns: %w[id data_date item_id pv uu],
      filter: -> (id, data_date, item_id, pv, uu) {
        [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
      }
    )
    job.execute
  end

  def test_dup_options
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      RedshiftConnector.transport_delta(
        schema: $TEST_SCHEMA,
        table: 'item_pvs',

        txn_id: data_date,
        condition: %Q(data_date = date '#{data_date}'),
        delete_cond: %Q(data_date = date '#{data_date}'),

        # Conflicts with delete_cond option
        upsert_columns: %w[pv uu],

        columns: %w[id data_date item_id pv uu],
        filter: -> (id, data_date, item_id, pv, uu) {
          [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
        }
      )
    }
  end

  def test_no_required_option
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      RedshiftConnector.transport_delta(
        schema: $TEST_SCHEMA,
        table: 'item_pvs',

        txn_id: data_date,
        condition: %Q(data_date = date '#{data_date}'),

        columns: %w[id data_date item_id pv uu],
        filter: -> (id, data_date, item_id, pv, uu) {
          [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
        }
      )
    }
  end

  def test_connector_rebuild_truncate
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_all(
      strategy: 'truncate',
      schema: $TEST_SCHEMA,
      table: 'item_pvs',
      txn_id: data_date,
      columns: %w[id data_date item_id pv uu],
      filter: -> (id, data_date, item_id, pv, uu) {
        [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
      }
    )
    job.execute
  end

  def test_connector_rebuild_rename
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_all(
      strategy: 'rename',
      schema: $TEST_SCHEMA,
      table: 'item_pvs',
      txn_id: data_date,
      columns: %w[id data_date item_id pv uu],
      filter: -> (id, data_date, item_id, pv, uu) {
        [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
      }
    )
    job.execute
  end

  def test_connector_src_dest_table
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_delta(
      schema: $TEST_SCHEMA,
      src_table: 'item_pvs',
      dest_table: 'item_pvs',

      txn_id: data_date,
      condition: %Q(data_date = date '#{data_date}'),

      columns: %w[id data_date item_id pv uu],
      upsert_columns: %w[pv uu],
      filter: -> (id, data_date, item_id, pv, uu) {
        [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
      }
    )
    job.execute
  end

  def test_connector_missing_src_dest
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      RedshiftConnector.transport_delta(
        schema: $TEST_SCHEMA,
        src_table: 'item_pvs',

        txn_id: data_date,
        condition: %Q(data_date = date '#{data_date}'),

        columns: %w[id data_date item_id pv uu],
        upsert_columns: %w[pv uu],
        filter: -> (id, data_date, item_id, pv, uu) {
          [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
        }
      )
    }
  end
end
