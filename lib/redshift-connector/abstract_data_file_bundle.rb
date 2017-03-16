module RedshiftConnector
  class AbstractDataFileBundle
    def each_row(&block)
      each_object do |obj|
        obj.each_row(&block)
      end
    end

    alias each each_row

    def each_object(&block)
      all_data_objects.each do |obj|
        @logger.info "processing s3 object: #{obj.key}"
        yield obj
      end
    end

    def all_data_objects
      data_files.select {|obj| obj.data_object? }
    end
  end
end
