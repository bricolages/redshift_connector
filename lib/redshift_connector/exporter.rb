module RedshiftConnector
  module Exporter
    @default_data_source = nil

    def Exporter.default_data_source=(ds)
      @default_data_source = ds
    end

    def Exporter.default_data_source
      @default_data_source or raise ArgumentError, "RedshiftConnector::Exporter.default_data_source was not set"
    end

    def Exporter.builder
      default_data_source.exporter_builder
    end

    def Exporter.for_table_delta(**params)
      builder.build_for_table_delta(**params)
    end

    def Exporter.for_table(**params)
      builder.build_for_table(**params)
    end

    def Exporter.for_query(**params)
      builder.build_for_query(**params)
    end

    def Exporter.foreach(**params, &block)
      exporter = for_query(**params)
      bundle = exporter.execute
      begin
        bundle.each_row(&block)
      ensure
        bundle.clear if bundle.respond_to?(:clear)
      end
    end
  end
end
