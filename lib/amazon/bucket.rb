require "aws-sdk"

module Amazon
  class Bucket
    def initialize(bucket_name, access_key_id, access_secret_key, region)
      @s3_resource = Aws::S3::Resource.new(
        :region => region,
        :access_key_id => access_key_id,
        :secret_access_key => access_secret_key
      )
      @bucket = @s3_resource.bucket(bucket_name)
      @region = region
    end

    attr_reader :region

    def get_folders(folder: "")
      objects = _list_objects(folder)

      objects["common_prefixes"].map(&:prefix)
    end

    def get_files_in_folder(folder)
      objects = _list_objects(folder)

      objects["contents"].map(&:key)
        .select { |file| file != folder }
        .map { |file| file.sub(folder, "") }
    end

    def get_file_link(folder, file_name)
      client = @s3_resource.client
      signer = Aws::S3::Presigner.new(:client => client)

      signer.presigned_url(
        :get_object,
        :bucket => @bucket.name,
        :key => "#{folder}#{file_name}"
      )
    end

    def url
      "s3://#{@bucket.name}"
    end

    def find_files(file)
      objects = @bucket.objects(
        :prefix => file
      )

      objects.map(&:key)
    end

    def add_extension_to_file(file_name, extension)
      new_file_name = "#{file_name}.#{extension}"
      object = @bucket.object(file_name)
      object.move_to(
        :bucket => @bucket.name,
        :key => new_file_name
      )
      new_file_name
    end

    def upload_file(object_name, local_file_name, content_type: nil)
      object = @bucket.object(object_name)
      if content_type.present?
        object.upload_file(local_file_name, :content_type => content_type)
      else
        object.upload_file(local_file_name)
      end
    end

    def upload_data(object_name, data, content_type: nil)
      object = @bucket.object(object_name)
      if content_type.present?
        object.put(:body => data, :content_type => content_type)
      else
        object.put(:body => data)
      end
    end

    def download_file(object_name, local_file_name)
      object = @bucket.object(object_name)
      object.download_file(local_file_name)
      local_file_name
    end

    def download_data(object_name)
      @bucket.object(object_name).get.body.read
    end

    def move_object(object_name, target_name)
      @bucket.object(object_name).move_to("#{@bucket.name}/#{target_name}")
    end

    private

    def _list_objects(prefix)
      client = @s3_resource.client
      client.list_objects(
        :bucket => @bucket.name,
        :prefix => prefix,
        :delimiter => "/",
        :encoding_type => "url"
      )
    end
  end

  class BucketSubDir
    def initialize(path, bucket)
      @bucket = bucket
      @path = path
    end

    def get_folders(folder: "")
      @bucket.get_folders(:folder => "#{@path}/#{folder}")
    end

    def get_files_in_folder(folder)
      @bucket.get_files_in_folder("#{@path}/#{folder}")
    end

    def upload(file_name, object_name: file_name)
      @bucket.upload_file("#{@path}/#{object_name}", file_name)
    end

    def download(object_name, file_name: object_name)
      @bucket.download_file("#{@path}/#{object_name}", file_name)
      file_name
    end

    def region
      @bucket.region
    end

    def url
      "#{@bucket.url}/#{@path}/"
    end
  end
end
