require "spec_helper"

RSpec.describe Amazon::Bucket do
  let(:subect) { Amazon::Bucket.new(SpecHelper::TESTING_BUCKET, SpecHelper::TESTING_ACCESS_KEY_ID, SpecHelper::TESTING_SECRET_ACCESS_KEY, SpecHelper::TESTING_REGION)}
  let(:method_stub) {-> {'stub'}}

  it "uploads data correctly with acl and content type" do
    file = Tempfile.open("out.txt") do |f|
      f.write("hello world")
      f
    end

    expect_any_instance_of(Aws::S3::Resource).to receive_message_chain(:bucket, :object).and_return(method_stub)
    expect(method_stub)
    .to receive(:put)
    .with({:body => file, :acl=>"public-read", :content_type=>"image/png"})
    .and_return(OpenStruct.new({:data => Object.new}))

    result = subect.upload_data("#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", file, :acl => "public-read", :content_type => "image/png")
    expect(result.data).to be
  end

  it "upload data correctly without acl or content type" do
    file = Tempfile.open("out.txt") do |f|
      f.write("hello world")
      f
    end

    expect_any_instance_of(Aws::S3::Resource).to receive_message_chain(:bucket, :object).and_return(method_stub)
    expect(method_stub)
    .to receive(:put)
    .with({:body => file})
    .and_return(OpenStruct.new({:data => Object.new}))

    result = subect.upload_data("#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", file)
    expect(result.data).to be
  end
end
