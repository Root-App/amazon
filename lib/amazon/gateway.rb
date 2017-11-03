module Amazon
  class Gateway
    module Strategy
      REAL = "real".freeze
      FAKE = "fake".freeze
    end

    def initialize(config)
      config.assert_valid_keys(:aws_access_key_id, :aws_access_secret_key, :region, :communicator_strategy)
      @aws_access_key_id = config[:aws_access_key_id]
      @aws_access_secret_key = config[:aws_access_secret_key]
      @region = config[:region]
      @communicator_strategy = config[:communicator_strategy]
    end

    def find_bucket(bucket_name)
      _communicator.find_bucket(bucket_name)
    end

    def find_bucket_subdir(path, bucket_name)
      _communicator.find_bucket_subdir(path, bucket_name)
    end

    def find_redshift_table(schema, table_name, redshift_url)
      _communicator.find_redshift_table(schema, table_name, redshift_url)
    end

    private

    def _communicator
      case @communicator_strategy
      when Strategy::REAL then RealCommunicator.new(@aws_access_key_id, @aws_access_secret_key, @region)
      when Strategy::FAKE then FakeCommunicator.new(@aws_access_key_id, @aws_access_secret_key, @region)
      else
        raise "invalid communicator strategy: #{@communicator_strategy.inspect}" # rubocop:disable RaiseI18n
      end
    end
  end
end
