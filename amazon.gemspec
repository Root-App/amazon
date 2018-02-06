Gem::Specification.new do |s|
  s.name    = "amazon"
  s.version = "0.0.9"
  s.authors = ["Root"]
  s.email   = ["devs@joinroot.com"]
  s.summary = "Integration with Amazon SDK"

  # external gems
  s.add_dependency "sequel"
  s.add_dependency "activesupport"
  s.add_dependency "aws-sdk"
  s.add_dependency "rspec-retry"
  s.add_dependency "pg"
end
