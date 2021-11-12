
module RedshiftConnector
  module RedshiftDataType
    FALSE_VALUES = [
      false, 0,
      "0", :"0",
      "f", :f,
      "F", :F,
      "false", :false,
      "FALSE", :FALSE,
      "off", :off,
      "OFF", :OFF,
    ].to_set.freeze

    def self.type_cast(row, manifest_file)
      row.zip(manifest_file.column_types).map do |value, type|
        next nil if (value == '' and type != 'character varing') # null becomes '' on unload

        case type
        when 'smallint', 'integer', 'bigint'
          value.to_i
        when 'numeric', 'double precision'
          value.to_f
        when 'character', 'character varying'
          value
        when 'timestamp without time zone', 'timestamp with time zone'
          value # Ruby does not have a class without timezone
        when 'date'
          Date.parse(value)
        when 'boolean'
          FALSE_VALUES.include?(value) ? false : true
        else
          raise "not support data type: #{type}"
        end
      end
    end
  end
end
