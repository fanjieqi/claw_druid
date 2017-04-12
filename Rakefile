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

task :tmp_test do |variable|
  client = ClawDruid.new("http://52.38.209.191:8092/druid/v2/", "publisher_daily_report")
  result = client.where(begin_date: "2017-02-02", end_date: "2017-02-05", game_cd: [1012, 1006]).group(:report_date, :game_cd).sum(:register_udid_count).order(:report_date, :game_cd).limit(10)
  # result = client.where(begin_date: "2017-02-02", end_date: "2017-02-03", game_cd: [1012, 1006]).page(1)
  
  # client = ClawDruid.new("http://52.38.209.191:8092/druid/v2/", "wikiticker")
  # result = client.where(begin_date: "2015-09-12", end_date: "2015-09-13").group(:page).sum(:edits).order(:edits).top(25)
  p result
end
