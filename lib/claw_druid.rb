require 'httparty' 
require 'json'
require 'awesome_print'

class ClawDruid
  THRESHOLD = 30
  OPRATIONS = {
    '<' => "lessThan",
    '>' => 'greaterThan',
    '=' => 'equalTo'
  }

  def initialize(url = "", source = "")
    @url    = url
    @params = {dataSource: source, granularity: "day", queryType: "select"}

    # The page_identifiers of every query, the key is the params.hash of the query, the value is a identifiers like "publisher_daily_report_2017-02-02T00:00:00.000Z_2017-02-04T00:00:00.000Z_2017-03-30T12:10:27.053Z"
    @paging_identifiers = {}
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
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column| 
      if column[/( [\+\-\*\/] )/]
        # split(/ [\+\-\*\/] /), and the result without the ' + ', ' - ', ' * ', ' / '
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         "sum(#{column})",
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { return current + (#{column}); }",
        }
      else
        { type: "doubleSum", name: "sum(#{column})", fieldName: column } 
      end
    }
    self
  end

  def max(*columns)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column| 
      if column[/( [\+\-\*\/] )/]
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         "max(#{column})",
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { return Math.max(current, (#{column})); }",
        }
      else
        { type: "doubleMax", name: "max(#{column})", fieldName: column } 
      end
    }
    self
  end

  def min(*columsn)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column| 
      if column[/( [\+\-\*\/] )/]
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         "min(#{column})",
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { return Math.max(current, (#{column})); }",
        }
      else
        { type: "doubleMin", name: "min(#{column})", fieldName: column } 
      end
    }
    self
  end

  def count
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    @params[:aggregations] << { type: "count", name: "count" }
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
    if page_count == 1
      @params[:pagingSpec] = {pagingIdentifiers: {}, threshold: THRESHOLD}
    else 
      current = @params.hash
      query(@params.merge(pagingSpec: {pagingIdentifiers:  {}, threshold: THRESHOLD})) unless @paging_identifiers[current]
      identifiers = @paging_identifiers[current]

      @params[:pagingSpec] = {pagingIdentifiers: {identifiers => (page_count - 1) * THRESHOLD }, threshold: THRESHOLD}
    end
    self
  end

  def having(*conditions)
    # Process the ('a = ? and b = ?', 1, 2)
    conditions[0].gsub!(" \?").each_with_index { |v, i| conditions[i + 1] }

    @params[:having] = having_chain(nil, conditions[0])
    ap @params
    self
  end

  def query(params = @params)
    ap @params
    result = HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' }).body
    
    # The result is a String, try to find the existence of substring 'pagingIdentifiers'.
    if result["pagingIdentifiers"]
      params.delete(:pagingSpec)
      current = params.hash

      @paging_identifiers[current] = JSON.parse(result)[0]["result"]["pagingIdentifiers"].keys[0]
    end
    ap JSON.parse(result)
    
    result
  end

  private
  
  def having_chain(relation, conditions)
    if relation.nil?
      if conditions[/[\(\))]/]
        # Todo
      elsif conditions[" and "] && !conditions[" or "]
        { type: "and", havingSpecs: conditions.split(" and ").delete_if{|condition| condition == " and "}.map{|condition| having_chain(nil, condition)} }
      elsif conditions[" or "]
        { type: "or", havingSpecs: conditions.split(" or ").delete_if{|condition| condition == " or "}.map{|condition| having_chain(nil, condition)} }
      else
        column, op, value = conditions.split(/( [\<\>\=] )/).map(&:strip)
        { type: OPRATIONS[op], aggregation: column, value: value }
      end
    else
      { type: relation, havingSpecs: conditions.map{|condition| having_chain(nil, condition)} }
    end
  end

end
