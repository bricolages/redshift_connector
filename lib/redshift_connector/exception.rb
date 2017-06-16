module RedshiftConnector
  class Error < ::StandardError; end
  class ExportError < Error; end
  class ImportError < Error; end
end
