require "spec_helper"

RSpec.describe Amazon::Bucket do
  let(:subect) { Amazon::Bucket.new(SpecHelper::TESTING_BUCKET, SpecHelper::TESTING_ACCESS_KEY_ID, SpecHelper::TESTING_SECRET_ACCESS_KEY, SpecHelper::TESTING_REGION)}
  let(:method_stub) {-> {'stub'}}

  it "should change the acl and get back an acknowledgement" do
    expect_any_instance_of(Aws::S3::Resource).to receive(:client).and_return(method_stub)
    expect(method_stub)
    .to receive(:put_object_acl)
    .with({:acl=>"public-read", :bucket=>SpecHelper::TESTING_BUCKET, :key=>"#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file"})
    .and_return(OpenStruct.new({:request_charged => "RequestCharged"}))

    result = subect.put_object_acl("#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", "public-read") 
    expect(result.request_charged).equal?("RequestCharged")
  end
end
