module RedshiftConnector
end

require 'redshift_connector/connector'
require 'redshift_connector/exporter'
require 'redshift_connector/active_record_data_source'
require 'redshift_connector/active_record_exporter'
require 'redshift_connector/importer'
require 'redshift_connector/s3_bucket'
require 'redshift_connector/s3_data_file_bundle'
require 'redshift_connector/exception'
require 'redshift_connector/version'

module RedshiftConnector
  def RedshiftConnector.transport_delta(**params)
    Connector.transport_delta(**params)
  end

  def RedshiftConnector.transport_all(**params)
    Connector.transport_all(**params)
  end

  def RedshiftConnector.transport_delta_from_s3(**params)
    Importer.transport_delta_from_s3(**params)
  end

  def RedshiftConnector.transport_all_from_s3(**params)
    Importer.transport_all_from_s3(**params)
  end

  def RedshiftConnector.foreach(**params, &block)
    Exporter.foreach(**params, &block)
  end
end
