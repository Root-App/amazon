module Amazon
  class RealCommunicator
    def initialize(access_key_id, secret_access_key, region)
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @region = region
    end

    def find_bucket(bucket_name)
      Amazon::Bucket.new(bucket_name, @access_key_id, @secret_access_key, @region)
    end

    def put_object_acl(bucket_name, object_key, acl)
      Amazon::Bucket.put_object_acl({
        acl: acl,
        bucket: bucket_name,
        key: object_key,
      })
    end

    def find_bucket_subdir(path, bucket_name)
      bucket = Amazon::Bucket.new(bucket_name, @access_key_id, @secret_access_key, @region)
      Amazon::BucketSubDir.new(path, bucket)
    end

    def find_redshift_table(schema, table_name, redshift_url)
      connection = Amazon::Redshift.connection(redshift_url)
      Amazon::Redshift::Table.new(schema, table_name, connection)
    end
  end
end
