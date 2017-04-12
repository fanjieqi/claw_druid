require 'httparty' 
require 'json'
require 'awesome_print'

class ClawDruid
  THRESHOLD = 10

  def initialize(url = "", source = "")
    @url    = url
    @params = {dataSource: source, granularity: "day", queryType: "select"}

    # The page_identifiers of every query, the key is the params.hash of the query, the value is a hash like {1 => identifiers1, 2 => identifiers2}
    @paging_identifiers = {}
    @current_page = nil
  end

  def group(*dimensions)
    @params[:queryType]  = "groupBy"
    if dimensions.count == 1
      @params[:dimension] = dimensions[0].to_s.strip
    else
      @params[:dimensions] = dimensions.map(&:to_s).map(&:strip)
    end
    self
  end

  def sum(*columns)
    @params[:aggregations] = columns.map{|column|
      { type: "doubleSum", name: column, fieldName: column }
    }
    self
  end

  def where(conditions)
    begin_date = conditions.delete(:begin_date)
    end_date = conditions.delete(:end_date)
    @params[:intervals] = ["#{begin_date}/#{end_date}"]

    if conditions.count > 1
      @params[:filter] = {
        type: "and",
        fields: conditions.map{|column, values|
          if values.count == 1
            { type: "selector", dimension: column, value: values }
          else
            {
              type: "or",
              fields: values.map{|value| {type: "selector", dimension: column, value: value} }
            }
          end
        }
      }
    else
      column = conditions.keys[0]
      values = conditions.values[0]
      if values.count == 1
        @params[:filter] = {type: "selector", dimension: column, value: values}
      else
        @params[:filter] = {
          type: "or",
          fields: values.map{|value| {type: "selector", dimension: column, value: value} }
        }
      end
    end
    self
  end

  def order(*columns)
    if @params[:queryType] == "groupBy"
      @params[:limitSpec] = {
        type: "default",
        limit: 500000,
        columns: columns
      }
    else
      @params[:metric] = columns
    end
    self
  end

  def limit(limit_count)
    @params[:limitSpec]         ||= {}
    @params[:limitSpec][:type]  ||= "default"
    @params[:limitSpec][:limit]   = limit_count
    self
  end

  def top(top_count)
    @params[:queryType] = "topN"
    @params[:threshold] = top_count
    @params[:metric] = @params.delete(:limitSpec)[:columns][0]
    self
  end

  def page(page_count)
    current = @params.hash
    @paging_identifiers[current] ||= {}
    if page_count == 1
      @params[:pagingSpec] = {pagingIdentifiers: {}, threshold: THRESHOLD}
    elsif @paging_identifiers[current][page_count - 1]
      @params[:pagingSpec] = {pagingIdentifiers: @paging_identifiers[current][page_count - 1], threshold: THRESHOLD}
    else
      last_threshold = THRESHOLD * (page_count - 1)
      last_page = query(@params.merge(pagingSpec: {pagingIdentifiers:  {}, threshold: last_threshold}))

      last_identifiers = last_page["result"]["pagingIdentifiers"]
      last_identifiers.each{|key, value| last_identifiers[key] += 1}

      @paging_identifiers[current][page_count - 1] = last_identifiers
      @params[:pagingSpec] = {pagingIdentifiers: last_identifiers, threshold: THRESHOLD}
      @current_page = page_count
    end
    self
  end

  def query(params = @params)
    HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' })
  end

  def inspect
    puts @params.to_json
    result = HTTParty.post(@url, body: @params.to_json, headers: { 'Content-Type' => 'application/json' })
    if result[:pagingIdentifiers]
      @params.delete(:pagingSpec)
      current = @params.hash
      @paging_identifiers[current][@current_page] = result[:pagingIdentifiers]
    end
    result
  end

end
