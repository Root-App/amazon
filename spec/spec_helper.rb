$LOAD_PATH << File.expand_path("../../lib", __FILE__)

require "rspec/retry"
require "amazon"

Dir[File.dirname(__FILE__) + "/support/**/*.rb"].each { |f| require f }

module SpecHelper
  TESTING_ACCESS_KEY_ID = "AMAZONKEYID".freeze
  TESTING_SECRET_ACCESS_KEY = "AMAZONSECRETACCESSKEY".freeze
  TESTING_REGION = "us-east-1".freeze
  TESTING_BUCKET = "example-bucket".freeze
  TESTING_BUCKET_SUBDIR = "public".freeze
  TESTING_REDSHIFT_URL = ENV["REDSHIFT_URL"].freeze
  TESTING_REDSHIFT_TABLE_SCHEMA = "server_public".freeze
  TESTING_REDSHIFT_TABLE_NAME = "accounts".freeze

  def self.real_config
    {
      :aws_access_key_id => TESTING_ACCESS_KEY_ID,
      :aws_access_secret_key => TESTING_SECRET_ACCESS_KEY,
      :region => TESTING_REGION,
      :communicator_strategy => "real"
    }
  end

  def self.fake_config
    {
      :aws_access_key_id => "TESTING_ACCESS_KEY_ID",
      :aws_access_secret_key => "TESTING_SECRET_ACCESS_KEY",
      :region => "TESTING_REGION",
      :communicator_strategy => "fake"
    }
  end
end
