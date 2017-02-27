# Redshift Connector for Rails

redshift-connector is a Redshift bulk data connector for Rails (ActiveRecord).

## Settings

Add following block to your Gemfile and bundle.
```
gem 'redshift-connector'
```
Add config/initializers/redshift-connector.rb like following:
```
module RedshiftConnector
  Exporter.default_data_source = Any_ActiveRecord_Class_Bound_To_Redshift

  S3Bucket.add('primary', default: true,
    region: 'YOUR_AWS_REGION_NAME',
    bucket: 'YOUR_BUCKET_NAME',
    prefix: 'YOUR_PREFIX',
    iam_role: 'arn:aws:iam::XXXXXXXXXXXX:role/RedshiftReadOnly'
    # For explicit S3 access, use following:
    # aws_access_key_id: 'XXXXXXXXXXXXX',
    # aws_secret_access_key: 'XXXXXXXXXXXXX'
  )
end
```

## Usage

### Fetching rows

```
RedshiftConnector.foreach(schema: 'app_mst', table: 'shops', query: 'select id, name from app_mst.shops') do |id, name|
  p [id, name]
end
```
`schema` and `table` is the source table name (written in the query).
This method executes Redshift UNLOAD statement with given query and
unload result to the intermediate S3, then read contents.
