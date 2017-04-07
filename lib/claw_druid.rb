require 'httparty'

class ClawDruid
  def initialize(url = "", source = "")
    @url    = url
    @source = source
  end

  def query(params = {})
    HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' })
  end
end
