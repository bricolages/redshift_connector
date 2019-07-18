require_relative 'lib/redshift_connector/version'

Gem::Specification.new do |s|
  s.platform = Gem::Platform::RUBY
  s.name = 'redshift_connector'
  s.version = RedshiftConnector::VERSION
  s.summary = 'Redshift bulk data connector'
  s.description = 'redshift_connector is a bulk data connector for Rails (ActiveRecord).'
  s.license = 'MIT'

  s.author = ['Minero Aoki']
  s.email = 'aamine@loveruby.net'
  s.homepage = 'https://github.com/bricolages/redshift_connector'

  s.files = `git ls-files -z`.split("\x0").reject {|f| f.match(%r{^(test|spec|features)/}) }
  s.require_path = 'lib'

  s.required_ruby_version = '>= 2.1.0'
  s.add_dependency 'activerecord'
  s.add_dependency 'activerecord-redshift'
  s.add_dependency 'pg', '~> 0.18.0'
  s.add_dependency 'activerecord-import'
  s.add_dependency 'redshift_csv_file', '~> 1.0'
  s.add_dependency 'aws-sdk-s3', '~> 1.0'
  s.add_development_dependency 'test-unit'
  s.add_development_dependency 'rake'
end
