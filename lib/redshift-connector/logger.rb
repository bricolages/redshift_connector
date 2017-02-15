module RedshiftConnector
  @logger = nil

  def RedshiftConnector.logger
    # Defer to access Rails
    @logger || Rails.logger
  end

  def RedshiftConnector.logger=(logger)
    @logger = logger
  end

  class NullLogger
    def noop(*args) end
    alias error noop
    alias warn noop
    alias info noop
    alias debug noop
  end
end
