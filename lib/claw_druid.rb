require 'httparty' 
require 'json'
require 'awesome_print'

class ClawDruid
  THRESHOLD = 30

  def initialize(url = "", source = "")
    @url    = url
    @params = {dataSource: source, granularity: "day", queryType: "select"}

    # The page_identifiers of every query, the key is the params.hash of the query, the value is a hash like {1 => identifiers1, 2 => identifiers2}
    @paging_identifiers = {}
    # The current_page to control the setting of @paging_identifiers when need to find paging_identifiers from last page
    @current_page = nil
  end

  def group(*dimensions)
    @params[:queryType]  = "groupBy"
    select(dimensions)
    @params.delete(:metrics)
    self
  end

  def select(*columns)
    if columns.count == 1
      @params[:dimension]   = columns[0].to_s.strip
      @params[:metrics]     = columns[0].to_s.strip if @params[:queryType] == "select"
    else
      @params[:dimensions]  = columns.map(&:to_s).map(&:strip)
      @params[:metrics]     = columns.map(&:to_s).map(&:strip) if @params[:queryType] == "select"
    end
    self
  end

  def sum(*columns)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
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
      if !values.is_a?(Array)
        @params[:filter] = {type: "selector", dimension: column, value: values}
      elsif values.count == 1
        @params[:filter] = {type: "selector", dimension: column, value: values[0]}
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
      @current_page = page_count - 1
      query(@params.merge(pagingSpec: {pagingIdentifiers:  {}, threshold: THRESHOLD * @current_page}))

      last_identifiers = @paging_identifiers[current][page_count - 1]
      @params[:pagingSpec] = {pagingIdentifiers: last_identifiers, threshold: THRESHOLD}
      @current_page = page_count
    end
    self
  end

  def query(params = @params)
    puts @params.to_json
    result = HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' }).body
    # The result is a String, try to find the existence of substring 'pagingIdentifiers'.
    if @current_page && result["pagingIdentifiers"]
      params.delete(:pagingSpec)
      current = params.hash

      last_identifiers = JSON.parse(result)[0]["result"]["pagingIdentifiers"]
      last_identifiers.each{|key, value| last_identifiers[key] += 1}
      @paging_identifiers[current][@current_page] = last_identifiers
      @current_page = nil
    end
    result
  end

end
