module RedshiftConnector
end

require 'redshift-connector/connector'
require 'redshift-connector/exporter'
require 'redshift-connector/importer'
require 'redshift-connector/s3_bucket'
require 'redshift-connector/s3_data_file_bundle'
require 'redshift-connector/version'

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
