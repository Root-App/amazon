require "rspec/core/rake_task"

namespace :spec do
  desc "runs gem specs"
  RSpec::Core::RakeTask.new(:run)
end

task :default => %w(spec:run)
