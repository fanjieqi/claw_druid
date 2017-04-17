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

  def initialize(params = {})
    @url        = params[:url]
    @params     = {dataSource: params[:source], granularity: "day", queryType: "select"}
    @threshold  = params[:threshold] || THRESHOLD

    # The page_identifiers of every query, the key is the params.hash of the query, the value is a identifiers like "publisher_daily_report_2017-02-02T00:00:00.000Z_2017-02-04T00:00:00.000Z_2017-03-30T12:10:27.053Z"
    @paging_identifiers = {}
  end

  def group(*dimensions)
    @params[:queryType]  = "groupBy"
    if dimensions && dimensions.count > 0
      @params[:dimensions] ||= []
      @params[:dimensions]  += dimensions.map(&:to_s).map(&:strip)
    end
    @params.delete(:metrics)
    self
  end

  def select(*columns)
    # Split the columns like ['sum(column_a) as sum_a, column_b']
    columns = columns[0].split("\, ") if columns.count == 1 && columns[0]["\, "]

    # Add the 'i' to regex to be case-insensitive, cause the sum, max and min could be SUM, MAX and MIN
    post_columns = columns.select{|column| column[/(sum|max|min).+[\+\-\*\/]/i] }
    @params[:postAggregations] = post_columns.map{|post_column| post_chain(post_column) } unless post_columns.empty?
    columns -= post_columns

    %w(sum max min).each do |method|
      tmp_columns = columns.select{|column| column[/#{method}/i] }
      unless tmp_columns.empty?
        columns -= tmp_columns
        tmp_columns.map! do |column| 
          column, naming = column.split(" as ")
          column.gsub!(/#{method}/i,"").gsub(/[\(\)]/,"")
          [column, naming]
        end
        send(method, *tmp_columns)
      end
    end

    if columns && columns.count > 0
      @params[:dimensions] ||= []
      @params[:dimensions]  += columns.map(&:to_s).map(&:strip)
      @params[:metrics]    ||= []
      @params[:metrics]     += columns.map(&:to_s).map(&:strip)# if @params[:queryType] == "select"
    end
    self
  end

  def sum(*columns)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column, naming| 
      naming       ||= "sum(#{column})"
      if column[/( [\+\-\*\/] )/]
        # split(/ [\+\-\*\/] /), and the result without the ' + ', ' - ', ' * ', ' / '
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         naming,
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { return current + (#{column}); }",
          fnCombine:    "function(partialA, partialB) { return partialA + partialB; }",
          fnReset:      "function()                   { return 0; }"
        }
      else
        { type: "doubleSum", name: naming, fieldName: column } 
      end
    }
    @params[:aggregations].uniq!
    self
  end

  def max(*columns)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column, naming| 
      naming       ||= "max(#{column})"
      if column[/( [\+\-\*\/] )/]
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         naming,
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { return Math.max(current, (#{column})); }",
          fnCombine:    "function(partialA, partialB) { return partialA + partialB; }",
          fnReset:      "function()                   { return 0; }"
        }
      else
        { type: "doubleMax", name: naming, fieldName: column } 
      end
    }
    @params[:aggregations].uniq!
    self
  end

  def min(*columsn)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column, naming| 
      naming       ||= "min(#{column})"
      if column[/( [\+\-\*\/] )/]
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         naming,
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { return Math.min(current, (#{column})); }",
          fnCombine:    "function(partialA, partialB) { return partialA + partialB; }",
          fnReset:      "function()                   { return 0; }"
        }
      else
        { type: "doubleMin", name: naming, fieldName: column } 
      end
    }
    @params[:aggregations].uniq!
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
          if values.empty?
            nil
          elsif values.count == 1
            { type: "selector", dimension: column, value: values }
          else
            { type: "or", fields: values.map{|value| {type: "selector", dimension: column, value: value} } }
          end
        }.compact
      }
    elsif conditions.count == 1
      column = conditions.keys[0]
      values = conditions.values[0]
      if !values.is_a?(Array)
        @params[:filter] = {type: "selector", dimension: column, value: values}
      elsif values.count == 1
        @params[:filter] = {type: "selector", dimension: column, value: values[0]}
      else
        @params[:filter] = {type: "or", fields: values.map{|value| {type: "selector", dimension: column, value: value} } }
      end
    end
    self
  end

  def order(*columns)
    columns = columns[0] if columns[0].is_a?(Hash)
    
    if @params[:queryType] != "groupBy"
      @params[:metric] ||= []
      @params[:metric] += columns.is_a?(Hash) ? columns.keys : columns.map{|column| column[0] }
    end
    @params[:limitSpec] = {
      type: "default",
      limit: 500000,
      columns: columns.map{|column, direction| 
        {
          dimension: (columns.is_a?(Hash) ? column : column[0]).to_s,
          direction: (columns.is_a?(Hash) ? direction : column[1]).to_s[/asc/i] ? "ascending" : "descending",
          dimensionOrder: "lexicographic"
        }
      }
    }
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
      @params[:pagingSpec] = {pagingIdentifiers: {}, threshold: @threshold}
    elsif page_count > 1
      current = @params.hash
      query(@params.merge(pagingSpec: {pagingIdentifiers:  {}, threshold: @threshold})) unless @paging_identifiers[current]
      identifiers = @paging_identifiers[current]

      @params[:pagingSpec] = {pagingIdentifiers: {identifiers => (page_count - 1) * @threshold }, threshold: @threshold}
    end
    self
  end

  def having(*conditions)
    # Process the ('a = ? and b = ?', 1, 2)
    conditions[0].gsub!(" \?").each_with_index { |v, i| " #{conditions[i + 1]}" }

    @params[:having] = having_chain(conditions[0])
    self
  end

  def query(params = @params)
    ap params if ENV['DEBUG']
    result = HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' }).body
    
    # The result is a String, try to find the existence of substring 'pagingIdentifiers'.
    if result["pagingIdentifiers"]
      params.delete(:pagingSpec)
      current = params.hash

      @paging_identifiers[current] = JSON.parse(result)[0]["result"]["pagingIdentifiers"].keys[0]
    end
    ap JSON.parse(result) if ENV['DEBUG']
    
    result
  end

  private
  
  def having_chain(conditions)
    # Todo: process the expression with brackets 
    if conditions[" and "] && !conditions[" or "]
      { type: "and", havingSpecs: conditions.split(" and ").delete_if{|condition| condition == " and "}.map{|condition| having_chain(condition)} }
    elsif conditions[" or "]
      { type: "or", havingSpecs: conditions.split(" or ").delete_if{|condition| condition == " or "}.map{|condition| having_chain(condition)} }
    else
      column, op, value = conditions.split(/( [\<\>\=] )/).map(&:strip)
      { type: OPRATIONS[op], aggregation: column, value: value }
    end
  end

  def post_chain(sentences)
    sentences, naming  = sentences.split(" as ")
    if sentences[/( (\+\+|\-\-|\*\*|\/\/) )/]
      # Todo: process the expression with brackets 
      if sentences[" ++ "]
        { type: "arithmetic", name: naming, fn: "+", fields: sentences.split(" ++ ").map{|sentence| post_chain(sentence)} }
      elsif sentences[" -- "]
        # Count the left part firstly, then substract the right part
        left, fn, right = sentences.rpartition(" -- ")
        { type: "arithmetic", name: naming, fn: "-", fields: [post_chain(left), post_chain(right)] }
      elsif sentences[" ** "]
        { type: "arithmetic", name: naming, fn: "*", fields: sentences.split(" ** ").map{|sentence| post_chain(sentence)} }
      elsif sentences[" // "]
        # Count the left part firstly, then devided by the right part
        left, fn, right = sentences.rpartition(" // ")
        { type: "arithmetic", name: naming, fn: "/", fields: [post_chain(left), post_chain(right)] }
      end
    else
      method    = sentences[/(sum|max|min)/i]
      sentences = sentences.gsub(method,"").gsub(/[\(\)]/,"")
      method.downcase!

      # Add the column to aggregations, which name is like sum(column), min(column), max(column)
      send(method, sentences)

      { type: "fieldAccess", name: naming, fieldName: "#{method}(#{sentences})" }
    end
  end

end
