module RedshiftConnector
  class Reader::Abstract
    READER_CLASSES = {}   # {Symbol => Class}

    def self.declare_reader(id)
      READER_CLASSES[id.to_sym] = self
    end

    def self.get_reader_class(id)
      READER_CLASSES[id.to_sym] or
          raise ArgumentError, "unknown data file reader type: #{id.inspect}"
    end
  end

  def self.get_reader_class(id)
    Reader::Abstract.get_reader_class(id)
  end
end
