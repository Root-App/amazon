module Amazon
  class FakeCommunicator
    def self.data
      @data ||= {}
    end
    def initialize(access_key_id, secret_access_key, region)
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @region = region
      @bucket_files
    end

    def find_bucket(bucket_name)
      FakeCommunicator.data[bucket_name] ||= {}
      @bucket_files = FakeCommunicator.data[bucket_name]
      _fake_bucket(bucket_name)
    end

    def find_bucket_subdir(path, bucket_name)
      bucket = _fake_bucket(bucket_name)

      bucket_subdir = Object.new
      bucket_subdir.instance_variable_set(:@bucket, bucket)
      bucket_subdir.instance_variable_set(:@path, path)

      def bucket_subdir.get_folders(folder: "")
        @bucket.get_folders(:folder => folder)
      end

      def bucket_subdir.get_files_in_folder(folder)
        @bucket.get_files_in_folder(folder)
      end

      def bucket_subdir.region
        @bucket.region
      end

      def bucket_subdir.url
        "#{@bucket.url}/#{@path}/"
      end

      bucket_subdir
    end

    def find_redshift_table(schema, table_name, redshift_url)
      redshift_table = Object.new
      redshift_table.instance_variable_set(:@schema, schema)
      redshift_table.instance_variable_set(:@table_name, table_name)

      def redshift_table.fetch_meta_data
        case
        when @schema == "fake_schema" || @table_name == "fake_table" then raise Sequel::DatabaseError
        else
          [
            {
              :column_name => "column_name",
              :data_type => "string",
              :comments => "comment on column name",
              :table_comments => "comments on table"
            }
          ]
        end
      end

      def redshift_table.unload_to_bucket(bucket, iam_role)
        "#{bucket.url}/public/account_facts/2017-05-05-public.account_facts000"
      end

      def redshift_table.reload_from_bucket(bucket, iam_role, encoding: "UTF8", ignore_header: true); end

      def redshift_table.fetch_column_names
        ["column_name_1", "column_name_2"]
      end

      redshift_table
    end

    private

    def _fake_bucket(bucket_name)
      raise Aws::Sigv4::Errors::MissingCredentialsError unless @access_key_id.present? && @secret_access_key.present?

      bucket = Object.new
      bucket.instance_variable_set(:@bucket_name, bucket_name)
      bucket.instance_variable_set(:@region, @region)
      bucket.instance_variable_set(:@bucket_files, @bucket_files)

      def bucket.get_folders(folder: "")
        case @bucket_name
        when "random_bucket" then raise Aws::S3::Errors.error_class('NoSuchBucket').new("test", "test")
        else
          return ["public/", "server_public/"] if folder.empty?
          ["#{folder}accounts", "#{folder}account_facts"]
        end
      end

      def bucket.get_files_in_folder(folder)
        case @bucket_name
        when "random_bucket" then raise Aws::S3::Errors.error_class('NoSuchBucket').new("test", "test")
        else
          if @bucket_files.present? && @bucket_files.keys.present?
            @bucket_files.keys
          else
            ["2015-05-12-file.csv", "2015-05-13-file.csv"]
          end
        end
      end

      def bucket.get_file_link(folder, file_name)
        "https://randomelink.url.2015-05-12.csv"
      end

      def bucket.url
        "s3://#{@bucket_name}"
      end

      def bucket.add_extension_to_file(file, extension)
        "#{file}.#{extension}"
      end

      def bucket.find_files(file)
        ["file_name.csv", "file_name.csv"]
      end

      def bucket.download_file(object_name, local_file_name)
        Tempfile.open(local_file_name) do |file|
          file.write(@bucket_files[object_name])
          file
        end
      end

      def bucket.upload_file(object_name, local_file_name, content_type: nil)
        @bucket_files[object_name] = File.read(local_file_name)
      end

      def bucket.region
        @region
      end

      bucket
    end

    def _raise_aws_error(error)
      s3 = Aws::S3::Client.new(stub_responses: true)
      s3.stub_responses(:list_objects, error)
      s3.list_objects(
        :bucket => "random_bucket"
      )
    end
  end
end
