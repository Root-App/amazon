RSpec.shared_examples_for "communicator_find_bucket" do
  context "with a valid bucket" do
    let(:bucket) { communicator.find_bucket(SpecHelper::TESTING_BUCKET) }

    it "should find folders" do
      file = Tempfile.open("out.txt") do |f|
        f.write("hello world")
        f
      end

      bucket.upload_file("public/account_facts/sample.file", file.path)

      folders = bucket.get_folders

      expect(folders).to be_present
      expect(folders).to be_an(Array)
    end

    it "should find files in folder" do
      file = Tempfile.open("out.txt") do |f|
        f.write("hello world")
        f
      end

      bucket.upload_file("public/account_facts/sample.file", file.path)

      files = bucket.get_files_in_folder("public/account_facts/")

      expect(files).to be_present
      expect(files).to be_an(Array)
    end

    it "should find files in folder when uploaded" do
      file = Tempfile.open("out.txt") do |f|
        f.write("hello world")
        f
      end
      bucket.upload_file("public/account_facts/sample.file", file.path)

      bucket = communicator.find_bucket(SpecHelper::TESTING_BUCKET)

      files = bucket.get_files_in_folder("public/account_facts/")

      expect(files).to be_present
      expect(files).to include("sample.file")
    end

    it "should get file link to s3" do
      link = bucket.get_file_link("public/account_facts/", "2017-05-13-public.account_facts.csv")

      expect(link).to be_present
      expect(link).to start_with("https://")
    end

    it "should upload and download a file to s3" do
      file = Tempfile.open("out.txt") do |f|
        f.write("hello world")
        f
      end
      bucket.upload_file("sample.file", file.path)

      downloaded_file = bucket.download_file("sample.file", "out.txt")
      expect(downloaded_file.class).to be(Tempfile)
      expect(downloaded_file.path).to include("out.txt")
    end

    context "move_to" do
      let(:file_path) { "my/file/path.file" }
      let(:new_path) { "new/file/path.file" }
      let(:file) do
        Tempfile.open("out.txt") do |f|
          f.write("hello world")
          f.path
        end
      end

      before { bucket.upload_file(file_path, file) }

      it "should move file" do
        bucket.move_object(file_path, new_path)

        files = bucket.get_files_in_folder("")

        expect(files).to include(new_path)
        expect(files).not_to include(file_path)
      end
    end
  end

  context "cannot find valid bucket" do
    let(:bad_bucket) { communicator.find_bucket("random_bucket") }

    it "should raise error with get_folders" do
      expect { bad_bucket.get_folders }.to raise_error(Aws::S3::Errors::NoSuchBucket)
    end

    it "should raise error with get_files_in_folder" do
      expect { bad_bucket.get_files_in_folder("folder") }.to raise_error(Aws::S3::Errors::NoSuchBucket)
    end
  end

  context "entered bad credentials" do
    it "should raise bad credentials error" do
      expect { bad_communicator.find_bucket(SpecHelper::TESTING_BUCKET) }.to raise_error(Aws::Sigv4::Errors::MissingCredentialsError)
    end
  end
end

RSpec.shared_examples_for "communicator_find_bucket_subdir" do
  context "with a valid bucket" do
    let(:bucket) { communicator.find_bucket(SpecHelper::TESTING_BUCKET) }
    let(:bucket_subdir) { communicator.find_bucket_subdir(SpecHelper::TESTING_BUCKET_SUBDIR, SpecHelper::TESTING_BUCKET) }

    it "should get folders" do
      file = Tempfile.open("out.txt") do |f|
        f.write("hello world")
        f
      end

      bucket.upload_file("#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", file.path)

      folders = bucket_subdir.get_folders

      expect(folders).to be_present
      expect(folders).to be_an(Array)
    end

    it "should get files in folder" do
      file = Tempfile.open("out.txt") do |f|
        f.write("hello world")
        f
      end

      bucket.upload_file("#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", file.path)

      files = bucket_subdir.get_files_in_folder("account_facts/")

      expect(files).to be_present
      expect(files).to be_an(Array)
    end

    it "should return a url" do
      url = bucket_subdir.url

      expect(url).to eq("s3://#{SpecHelper::TESTING_BUCKET}/#{SpecHelper::TESTING_BUCKET_SUBDIR}/")
    end
  end

  context "cannot find valid bucket" do
    let(:bad_bucket) { communicator.find_bucket_subdir("random_path", "random_bucket") }

    it "should raise error with get_folders" do
      expect { bad_bucket.get_folders }.to raise_error(Aws::S3::Errors::NoSuchBucket)
    end

    it "should raise error with get_files_in_folder" do
      expect { bad_bucket.get_files_in_folder("folder") }.to raise_error(Aws::S3::Errors::NoSuchBucket)
    end
  end

  context "entered bad credentials" do
    it "should raise bad credentials error" do
      expect { bad_communicator.find_bucket_subdir(SpecHelper::TESTING_BUCKET_SUBDIR, SpecHelper::TESTING_BUCKET) }.to raise_error(Aws::Sigv4::Errors::MissingCredentialsError)
    end
  end
end

RSpec.shared_examples_for "communicator_put_object_acl" do
  let(:bucket) { communicator.find_bucket(SpecHelper::TESTING_BUCKET) }
  
  it "should change the acl and get back an acknowledgement" do
    file = Tempfile.open("out.txt") do |f|
      f.write("hello world")
      f
    end
    bucket.upload_file("#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", file.path)
    result = communicator.put_object_acl(SpecHelper::TESTING_BUCKET, "#{SpecHelper::TESTING_BUCKET_SUBDIR}/sample.file", "public-read") 
    expect(result.request_charged).equal?("RequestCharged")
  end
end

RSpec.shared_examples_for "communicator_find_redshift" do
  context "valid redshift" do
    let(:valid_table) { communicator.find_redshift_table(SpecHelper::TESTING_REDSHIFT_TABLE_SCHEMA, SpecHelper::TESTING_REDSHIFT_TABLE_NAME, SpecHelper::TESTING_REDSHIFT_URL) }
    let(:invalid_table) { communicator.find_redshift_table("fake_schema", "fake_table", SpecHelper::TESTING_REDSHIFT_URL) }

    it "should fetch columns and meta data for valid table" do
      results = valid_table.fetch_meta_data

      expect(results).to be_present
      expect(results.length).to be > 0

      results.each do |result|
        expect(result).to have_key(:column_name)
        expect(result).to have_key(:data_type)
        expect(result).to have_key(:comments)
        expect(result).to have_key(:table_comments)
      end
    end

    it "passes in a bad schema and/or databasae" do
      expect { invalid_table.fetch_meta_data }.to raise_error(Sequel::DatabaseError)
    end
  end
end
