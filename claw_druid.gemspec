Gem::Specification.new do |s|
  s.name        = 'claw_druid'
  s.version     = '0.0.5'
  s.date        = Date.today.to_s
  s.summary     = "A ruby client of Druid."
  s.description = "A ruby client of Druid."
  s.authors     = ["Fan Jieqi"]
  s.email       = 'fjqqiqi@163.com'
  s.files       = Dir['lib/*.rb']
  s.homepage    = 'https://github.com/fanjieqi/claw_druid'
  s.license     = 'MIT'

  s.add_dependency 'rake', '~> 12.0'
  s.add_dependency 'httparty', '~> 0.14.0'
  s.add_dependency 'json', '~> 1.7'
  s.add_dependency 'awesome_print', '~> 1.7'
  s.add_dependency 'activesupport', '~> 4.2.3'
end
