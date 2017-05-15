require 'httparty' 
require 'json'
require 'awesome_print'
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/hash/transform_values'
require './lib/array'

class ClawDruid
  include Enumerable

  THRESHOLD = ENV["DEBUG"] ? 5 : 30
  OPERATIONS = {
    '<' => "lessThan",
    '>' => 'greaterThan',
    '=' => 'equalTo'
  }

  def initialize(params = {})
    @url        = params[:url]
    @params     = {dataSource: params[:source], granularity: "all", queryType: "select"}
    @threshold  = params[:threshold] || THRESHOLD

    # The page_identifiers of every query, the key is the params.hash of the query, the value is a identifiers like "publisher_daily_report_2017-02-02T00:00:00.000Z_2017-02-04T00:00:00.000Z_2017-03-30T12:10:27.053Z"
    @paging_identifiers = {}
  end

  def group(*dimensions)
    dimensions = dimensions[0] if dimensions.count == 1 && dimensions[0].is_a?(Array)

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
    columns = columns[0].split("\, ") if columns.count == 1 && columns[0].is_a?(String) && columns[0]["\, "]
    columns = columns[0]              if columns.count == 1 && columns[0].is_a?(Array)

    return self if columns.all?{|column| column.blank? }

    # Add the 'i' to regex to be case-insensitive, cause the sum, max and min could be SUM, MAX and MIN
    post_columns = columns.except{|column| column[/(sum|max|min).+[\+\-\*\/]/i] }
    @params[:postAggregations] = post_columns.map{|post_column| post_chain(post_column) } unless post_columns.blank?

    %w(sum max min).each do |method|
      tmp_columns = columns.select{|column| column.is_a?(String) && column[/#{method}/i] }
      unless tmp_columns.blank?
        columns -= tmp_columns
        tmp_columns.map! do |column| 
          column, naming = column.split(" as ")
          column.gsub!(/#{method}/i,"").gsub(/[\(\)]/,"")
          [column, naming]
        end
        send(method, *tmp_columns)
      end
    end

    lookup_columns = columns.except{|column| column.is_a? Hash }
    select_lookup(lookup_columns)
    
    if columns && columns.count > 0
      @params[:metrics]    ||= []
      @params[:metrics]     += columns.map(&:to_s).map(&:strip)
    end
    self
  end

  def sum(*columns)
    columns = columns[0] if columns.count == 1 and columns[0].is_a?(Array)

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
    columns = columns[0] if columns.count == 1 and columns[0].is_a?(Array)

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

  def min(*columns)
    columns = columns[0] if columns.count == 1 and columns[0].is_a?(Array)

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

  def count(*columns)
    @params[:queryType] = "timeseries" if @params[:queryType] != "groupBy"
    @params[:aggregations] ||= []
    if columns.empty?
      @params[:aggregations] << { type: "count", name: "count" }
    elsif columns.count == 1
      @params[:aggregations] << { type: "cardinality", name: "count(#{columns[0]})", fields: columns }
    else
      @params[:aggregations] += columns.map{|column| { type: "cardinality", name: "count(#{column})", fields: [column] } }
    end
    self
  end

  def where(conditions)
    if conditions.is_a?(Hash)
      begin_date = conditions.delete(:begin_date)
      end_date = conditions.delete(:end_date)
      @params[:intervals] = ["#{begin_date}/#{end_date}"]

      conditions = conditions.delete_if{|key, value| value.blank?}.map{|column, values|
        if !values.is_a?(Array)
          { type: "selector", dimension: column, value: values }
        elsif values.count == 1
          { type: "selector", dimension: column, value: values[0] }
        else
          { type: "in", dimension: column, values: values }
        end
      }.compact
    elsif conditions.is_a?(Array) && conditions[0][" \?"]
      conditions[0].gsub!(" \?").each_with_index { |v, i| " #{conditions[i + 1]}" }
      conditions = [where_chain( conditions[0] )]
    elsif conditions.is_a?(String)
      conditions = [where_chain( conditions )]
    end

    unless conditions.blank?
      @params[:filter]          ||= { type: "and", fields: [] }
      @params[:filter][:fields]  += conditions
    end
    self
  end

  def order(*columns)
    columns = columns[0] if columns[0].is_a?(Hash) || columns[0].is_a?(Array)
    
    if @params[:queryType] != "groupBy"
      @params[:metric] ||= []
      @params[:metric] += columns.is_a?(Hash) ? columns.keys : columns.map{|column| column[0] }
      @params[:descending] = columns.any?{|column, direction| direction.to_s[/desc/]}
    end
    @params[:limitSpec] = {
      type: "default",
      limit: 500000,
      columns: columns.map{|column, direction| 
        {
          dimension: (columns.is_a?(Hash) ? column : column).to_s,
          direction: (columns.is_a?(Hash) ? direction : "asc").to_s[/asc/i] ? "ascending" : "descending",
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
    @params[:metric] = @params.delete(:limitSpec)[:columns][0] if @params[:limitSpec]
    self
  end

  def page(page_count)
    if page_count == 1
      @params[:pagingSpec] = {pagingIdentifiers: {}, threshold: @threshold}
    elsif page_count > 1
      current = @params.hash
      @paging_identifiers[current] ||= {0 => {}}

      (1..page_count-1).each do |current_page|
        if begin @paging_identifiers[current][current_page].nil? rescue true end
          query(@params.merge(pagingSpec: {pagingIdentifiers: @paging_identifiers[current][current_page-1], threshold: @threshold}), current_page)
        end
      end if begin @paging_identifiers[current][page_count - 1].nil? rescue true end

      @params[:pagingSpec] = {pagingIdentifiers: @paging_identifiers[current][page_count - 1], threshold: @threshold}
    end
    self
  end

  def having(*conditions)
    # Process the ('a = ? and b = ?', 1, 2)
    conditions[0].gsub!(" \?").each_with_index { |v, i| " #{conditions[i + 1]}" }

    havings = having_chain(conditions[0])
    @params[:having] = havings unless havings.blank?
    
    self
  end

  def query(params = @params, page_count = nil)
    ap params if ENV['DEBUG']
    puts params.to_json if ENV['DEBUG']
    result = HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' }).body

    # The result is a String, try to find the existence of substring 'pagingIdentifiers'.
    if page_count && result["pagingIdentifiers"]
      params.delete(:pagingSpec)
      current = params.hash

      # The pagingIdentifiers is something like { "publisher_daily_report_2017-03-01T00:00:00.000Z_2017-03-11T00:00:00.000Z_2017-04-17T21:04:30.804Z" => -10 }
      @paging_identifiers[current]            ||= {}
      @paging_identifiers[current][page_count]  = JSON.parse(result)[0]["result"]["pagingIdentifiers"].transform_values{|value| value + 1}
    end
    # ap JSON.parse(result) if ENV['DEBUG']
    
    result
  end

  def time_boundary
    @params[:queryType] = "timeBoundary"
    self
  end

  def max_time
    @params[:queryType] = "timeBoundary"
    @params[:bound]     = "maxTime"
    self
  end

  def min_time
    @params[:queryType] = "timeBoundary"
    @params[:bound]     = "minTime"
    self
  end

  def source_meta
    @params[:queryType] = "dataSourceMetadata"
    self
  end

  def segment_meta
    @params[:queryType] = "segmentMetadata"
    self
  end

  def to_s
    query
  end

  def to_a
    JSON.parse(query)[0]["result"]["events"]
  end

  def each(&block)
    to_a.each(&block)
  end

  def map(&block)
    to_a.map(&block)
  end

  private

  def where_chain(conditions)
    # Todo: process the expression with brackets 
    if conditions[" or "]
      { type: "or", fields: conditions.split(" or ").delete_if{|condition| condition == " or "}.map{|condition| where_chain(condition)} }
    elsif conditions[" and "]
      { type: "and", fields: conditions.split(" and ").delete_if{|condition| condition == " and "}.map{|condition| where_chain(condition)} }
    elsif conditions[" in "]
      column, op, values = conditions.split(/( (in) )/).map(&:strip)
      { type: "in", dimension: column, values: JSON.parse(values) }
    else
      column, op, value = conditions.split(/( [\<\>\=\~] )/).map(&:strip)
      case op
      when "=" then { type: "selector", dimension: column, value: value }
      when ">" then { type: "bound", dimension: column, lower: value, ordering: "numeric" }
      when "<" then { type: "bound", dimension: column, upper: value, ordering: "numeric" }
      when "~" then value = JSON.parse(value); { type: "bound", dimension: column, lower: value[0], upper: value[1], ordering: "numeric"}
      else nil
      end
    end
  end
  
  def having_chain(conditions)
    # Todo: process the expression with brackets 
    if conditions[" and "] && !conditions[" or "]
      { type: "and", havingSpecs: conditions.split(" and ").delete_if{|condition| condition == " and "}.map{|condition| having_chain(condition)} }
    elsif conditions[" or "]
      { type: "or", havingSpecs: conditions.split(" or ").delete_if{|condition| condition == " or "}.map{|condition| having_chain(condition)} }
    elsif conditions[/[\<\>\=]/]
      column, op, value = conditions.split(/( [\<\>\=] )/).map(&:strip)
      { type: OPERATIONS[op], aggregation: column, value: value.to_f }
    else
      nil
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

  def select_lookup(columns)
    if columns.present?
      @params[:dimensions] ||= []
      @params[:dimensions]  += columns.map{|columns|
        {
          type:       "lookup",
          dimension:  columns[:dimension] || columns["dimension"],
          outputName: columns[:output],
          name:       columns[:lookup] || columns["lookup"],
        }
      }
    end
  end

end
