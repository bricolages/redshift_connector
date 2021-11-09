require_relative 'helper'
require 'test/unit'

class TestTypeCastOption < Test::Unit::TestCase
  def test_pass_cast_option_delta
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
      },
      enable_cast: false
    )
    job.execute
  end

  def test_not_support_cast_delta
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      job = RedshiftConnector.transport_delta(
        schema: $TEST_SCHEMA,
        table: 'item_pvs',

        txn_id: data_date,
        condition: %Q(data_date = date '#{data_date}'),
        delete_cond: %Q(data_date = date '#{data_date}'),

        columns: %w[id data_date item_id pv uu],
        enable_cast: true
      )
    }
  end

  def test_dup_options_delta
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      job = RedshiftConnector.transport_delta(
        schema: $TEST_SCHEMA,
        table: 'item_pvs',

        txn_id: data_date,
        condition: %Q(data_date = date '#{data_date}'),
        delete_cond: %Q(data_date = date '#{data_date}'),

        columns: %w[id data_date item_id pv uu],
        filter: -> (id, data_date, item_id, pv, uu) {
          [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
        },
        enable_cast: true
      )
    }
  end

  def test_non_option_delta
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      job = RedshiftConnector.transport_delta(
        schema: $TEST_SCHEMA,
        table: 'item_pvs',

        txn_id: data_date,
        condition: %Q(data_date = date '#{data_date}'),
        delete_cond: %Q(data_date = date '#{data_date}'),

        columns: %w[id data_date item_id pv uu],
      )
    }
  end

  def test_pass_cast_option_all
    data_date = '2016-11-03'
    job = RedshiftConnector.transport_all(
      strategy: 'truncate',
      schema: $TEST_SCHEMA,
      table: 'item_pvs',
      txn_id: data_date,
      columns: %w[id data_date item_id pv uu],
      filter: -> (id, data_date, item_id, pv, uu) {
        [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
      },
      enable_cast: false
    )
    job.execute
  end

  def test_not_support_cast_all
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      job = RedshiftConnector.transport_all(
        strategy: 'truncate',
        schema: $TEST_SCHEMA,
        table: 'item_pvs',
        txn_id: data_date,
        columns: %w[id data_date item_id pv uu],
        enable_cast: true
      )
    }
  end

  def test_dup_options_all
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      job = RedshiftConnector.transport_all(
        strategy: 'truncate',
        schema: $TEST_SCHEMA,
        table: 'item_pvs',
        txn_id: data_date,
        columns: %w[id data_date item_id pv uu],
        filter: -> (id, data_date, item_id, pv, uu) {
          [id.to_i, data_date, item_id.to_i, pv.to_i, uu.to_i]
        },
        enable_cast: true
      )
    }
  end

  def test_non_option_all
    data_date = '2016-11-03'
    assert_raise(ArgumentError) {
      job = RedshiftConnector.transport_all(
        strategy: 'truncate',
        schema: $TEST_SCHEMA,
        table: 'item_pvs',
        txn_id: data_date,
        columns: %w[id data_date item_id pv uu],
      )
    }
  end
end
