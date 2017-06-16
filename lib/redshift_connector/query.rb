module RedshiftConnector
  class DeltaQuery
    def initialize(schema:, table:, columns:, condition: nil)
      @schema = schema
      @table = table
      @columns = columns
      @condition = condition
    end

    def table_spec
      "#{@schema}.#{@table}"
    end

    def description
      "#{table_spec} (#{@columns.join(', ')}) where (#{@condition})"
    end

    def to_sql
      "select #{@columns.map {|c| %Q("#{c}") }.join(', ')}" \
          + " from #{table_spec}" \
          + (@condition ? " where #{@condition}" : '')
    end
  end

  class SelectAllQuery
    def initialize(schema:, table:, columns:)
      @schema = schema
      @table = table
      @columns = columns
    end

    def table_spec
      "#{@schema}.#{@table}"
    end

    def description
      "#{table_spec} (#{@columns.join(', ')})"
    end

    def to_sql
      "select #{@columns.map {|c| %Q("#{c}") }.join(', ')}" \
          + " from #{table_spec}"
    end
  end

  class UnloadQuery
    def UnloadQuery.wrap(query:, bundle:, enable_sort: false)
      new(query: ArbitraryQuery.new(query), bundle: bundle, enable_sort: enable_sort)
    end

    def initialize(query:, bundle:, enable_sort: false)
      @query = query
      @bundle = bundle
      @enable_sort = enable_sort
    end

    def table_spec
      @query.table_spec
    end

    def description
      @query.description
    end

    def to_sql
      <<-EndSQL.gsub(/^\s+/, '')
        unload ('#{escape_query(@query.to_sql)}')
        to '#{@bundle.url}'
        credentials '#{@bundle.credential_string}'
        gzip
        allowoverwrite
        parallel #{@enable_sort ? 'off' : 'on'}
        delimiter ',' escape addquotes
      EndSQL
    end

    def escape_query(query)
      query.gsub("'", "\\\\'")
    end
  end

  class ArbitraryQuery
    def initialize(query)
      @query = query
    end

    def description
      @query
    end

    def to_sql
      @query
    end
  end
end
