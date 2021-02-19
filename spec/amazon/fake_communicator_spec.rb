require "spec_helper"

RSpec.describe Amazon::FakeCommunicator do
  let(:communicator) { described_class.new(SpecHelper::TESTING_ACCESS_KEY_ID, SpecHelper::TESTING_SECRET_ACCESS_KEY, SpecHelper::TESTING_REGION) }
  let(:other_communicator) { described_class.new("", "", SpecHelper::TESTING_REGION) }
  let(:bad_communicator) { described_class.new(SpecHelper::TESTING_ACCESS_KEY_ID, "", SpecHelper::TESTING_REGION) }
  let(:other_bad_communicator) { described_class.new("", SpecHelper::TESTING_SECRET_ACCESS_KEY, SpecHelper::TESTING_REGION) }

  describe "#find_bucket" do
    it_behaves_like "communicator_find_bucket"
  end

  describe "#find_redshift" do
    it_behaves_like "communicator_find_redshift"
  end

  describe "#find_bucket_subdir" do
    it_behaves_like "communicator_find_bucket_subdir"
  end
end
