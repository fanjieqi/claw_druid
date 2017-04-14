require 'bundler/gem_tasks'
require 'claw_druid'
require 'awesome_print'

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
  p Time.now
  client = ClawDruid.new("http://52.38.209.191:8092/druid/v2/", "publisher_daily_report")
  # result = client.where(begin_date: "2017-02-02", end_date: "2017-02-05", game_cd: [1012, 1006]).group(:report_date, :game_cd).sum(:register_udid_count).order(:report_date, :game_cd).limit(10)
  # result = client.where(begin_date: "2017-02-02", end_date: "2017-02-03", game_cd: 1012).select(:publisher_name, :package_cd, :package_name, :game_cd, :click_count).page(3)
  
  # client = ClawDruid.new("http://52.38.209.191:8092/druid/v2/", "wikiticker")
  # result = client.where(begin_date: "2015-09-12", end_date: "2015-09-13").group(:page).sum(:edits).order(:edits).top(25)
  # puts result.query
  # client.having("click_count > ? and impression_count > ?", 0, 0)
  # client.having("click_count > 2")
  # client.having("click_count < ?", 2)
  # p client.having_chain("a = 1 and b = 2 or c = 3 and (d = 4 or e = 5")
  p client.having("a = 1 or b = 2 and c = 3 or d = 4 or e = 5 and f = 6")
  p Time.now
end
