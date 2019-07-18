require 'aws-sdk-s3'

module RedshiftConnector
  class S3Bucket
    @buckets = {}
    @default = nil

    def S3Bucket.add(name, default: false, **params)
      instance = new(**params)
      @buckets[name.to_s] = instance
      if !@default or default
        @default = instance
      end
    end

    def S3Bucket.default
      @default or raise ArgumentError, "no default S3 bucket configured"
    end

    def S3Bucket.get(name)
      @buckets[name.to_s] or raise ArgumentError, "no such S3 bucket configured: #{name.inspect}"
    end

    def initialize(region: nil, bucket:, prefix: nil, access_key_id: nil, secret_access_key: nil, iam_role: nil)
      @region = region
      @name = bucket
      @prefix = prefix
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @iam_role = iam_role
    end

    attr_reader :name
    attr_reader :prefix

    def url
      "s3://#{@bucket.name}/#{@prefix}/"
    end

    def client
      @client ||= begin
        args = { region: @region, access_key_id: @access_key_id, secret_access_key: @secret_access_key }.reject {|k, v| v.nil? }
        Aws::S3::Client.new(**args)
      end
    end

    def bucket
      @bucket ||= begin
        resource = Aws::S3::Resource.new(client: client)
        resource.bucket(@name)
      end
    end

    def object(key)
      bucket.object(key)
    end

    def objects(prefix:)
      bucket.objects(prefix: prefix)
    end

    def delete_objects(keys)
      bucket.delete_objects(delete: {objects: keys.map {|k| {key: k} }})
    end

    def credential_string
      if @iam_role
        "aws_iam_role=#{@iam_role}"
      elsif @access_key_id
        "aws_access_key_id=#{@access_key_id};aws_secret_access_key=#{@secret_access_key}"
      else
        raise ArgumentError, "no credential given for Redshift S3 access"
      end
    end
  end
end
