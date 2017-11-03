require "spec_helper"

RSpec.describe Amazon::Gateway do
  context "with the real communicator strategy" do
    let(:gateway) { described_class.new(SpecHelper.real_config) }

    it "users the real communicator" do
      expect(gateway.send(:_communicator)).to be_kind_of(Amazon::RealCommunicator)
    end
  end

  context "with the real communicator strategy" do
    let(:gateway) { described_class.new(SpecHelper.fake_config) }

    it "users the real communicator" do
      expect(gateway.send(:_communicator)).to be_kind_of(Amazon::FakeCommunicator)
    end
  end
end
