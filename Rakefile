require 'bundler/gem_tasks'
require 'claw_druid'

task :my_test do |variable|
	client = ClawDruid.new("http://52.38.209.191:8092/druid/v2/", "publisher_daily_report")
  params = {
    queryType: "segmentMetadata",
    dataSource: "publisher_daily_report",
    intervals: ["2017-01-01/2017-01-02"]
  }
  puts client.query(params)
end
