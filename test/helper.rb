require 'active_record'
require 'yaml'
require 'logger'

YAML.load_file("#{__dir__}/database.yml").each do |name, ent|
  ActiveRecord::Base.configurations[name] = ent
end

class BaseConn < ActiveRecord::Base
  establish_connection :mysql
  self.abstract_class = true
end
class ItemPv < BaseConn
  connection
end
class Redshift < ActiveRecord::Base
  establish_connection :redshift
end

require 'redshift_connector'
# This IS REQUIRED to ensure to load mysql2 driver, at least outside of Rails.
ActiveRecord::Import.require_adapter 'mysql2'
require_relative 'config'

RedshiftConnector.logger = Logger.new($stderr)
