require 'httparty' 
require 'json'
require 'awesome_print'
require 'active_support/all'
require_relative './array'

class ClawDruid
  include Enumerable

  THRESHOLD = ENV["DEBUG"] ? 5 : 30
  OPERATIONS = {
    '<' => "lessThan",
    '>' => 'greaterThan',
    '=' => 'equalTo'
  }

  FnAggregates = {
    "min" => "return Math.min(current, (COLUMN));",
    "max" => "return Math.max(current, (COLUMN));",
    "sum" => "return current + (COLUMN);"
  }

  TopN = "topN"
  Select = "select"
  GroupBy = "groupBy"
  TimeSeries = "timeseries"
  TimeBoundary = "timeBoundary"
  SegmentMetaData = "segmentMetadata"
  DataSourceMetaData = "dataSourceMetadata"

  Permit_Properties = {
    TopN => [:queryType, :dataSource, :intervals, :granularity, :filter, :aggregations, :postAggregations, :dimension, :threshold, :metric, :context],
    Select => [:queryType, :dataSource, :intervals, :granularity, :descending, :filter, :dimensions, :metrics, :pagingSpec, :context],
    GroupBy => [:queryType, :dataSource, :dimensions, :limitSpec, :having, :granularity, :filter, :aggregations, :postAggregations, :intervals, :context],
    TimeSeries => [:queryType, :dataSource, :descending, :intervals, :granularity, :filter, :aggregations, :postAggregations, :context],
    TimeBoundary => [:queryType, :dataSource, :bound, :filter, :context],
    SegmentMetaData => [:queryType, :dataSource, :intervals, :toInclude, :merge, :context, :analysisTypes, :lenientAggregatorMerge],
    DataSourceMetaData => [:queryType, :dataSource, :context],
  }

  def initialize(params = {})
    @url        = params[:url]
    @params     = {dataSource: params[:source], granularity: "all", queryType: Select}
    @threshold  = params[:threshold] || THRESHOLD

    # The page_identifiers of every query, the key is the params.hash of the query, the value is a identifiers like "publisher_daily_report_2017-02-02T00:00:00.000Z_2017-02-04T00:00:00.000Z_2017-03-30T12:10:27.053Z"
    @paging_identifiers = {}
  end

  def group(*dimensions)
    dimensions = dimensions[0] if dimensions.count == 1 && dimensions[0].is_a?(Array)

    @params[:queryType]  = GroupBy

    lookup_dimensions = dimensions.except{|dimension| dimension.is_a? Hash }
    select_lookup(lookup_dimensions)

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
    post_columns = columns.except{|column| column[/(sum|max|min|count).+[\+\-\*\/]/i] }
    @params[:postAggregations] = post_columns.map{|post_column| post_chain(post_column) } unless post_columns.blank?

    method_columns = columns.except{|column| column.is_a?(String) && column[/(sum|max|min|count)\(.+\)/i] }
    method_columns.each{|column| method_column(column) }

    lookup_columns = columns.except{|column| column.is_a? Hash }
    select_lookup(lookup_columns)
    
    if columns && columns.count > 0
      @params[:metrics]    ||= []
      @params[:metrics]     += columns.map(&:to_s).map(&:strip)
    end
    self
  end

  def meta_method(method, columns)
    columns = columns[0] if columns.count == 1 and columns[0].is_a?(Array)

    @params[:queryType]    ||= TimeSeries
    @params[:aggregations] ||= []
    @params[:aggregations] += columns.map{|column, naming| 
      naming       ||=  "#{method}(#{column})"
      fnAggregate    =  FnAggregates[method.to_s].gsub("COLUMN", column.to_s)
      if column[/( [\+\-\*\/] )/]
        fields = column.split(/ [\+\-\*\/] /)
        {
          type:         "javascript",
          name:         naming,
          fieldNames:   fields,
          fnAggregate:  "function(current, #{fields.join(', ')}) { #{fnAggregate} }",
          fnCombine:    "function(partialA, partialB) { return partialA + partialB; }",
          fnReset:      "function()                   { return 0; }"
        }
      else
        { type: "double#{method.capitalize}", name: naming, fieldName: column } 
      end
    }
    @params[:aggregations].uniq!
    self
  end

  [:min, :max, :sum].each do |method|
    define_method(method) do |*columns|
      meta_method(method, columns)
    end
  end

  def count(*columns)
    @params[:queryType]    ||= TimeSeries
    @params[:aggregations] ||= []
    if columns.empty?
      @params[:aggregations] << { type: "count", name: "count" }
    else
      @params[:aggregations] += columns.map{|column| { type: "cardinality", name: "count(#{column})", fields: [column] } }
    end
    self
  end

  def where(*conditions)
    if conditions[0].is_a?(Hash)
      conditions = conditions[0]
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
    elsif conditions[0].is_a?(String)
      conditions[0].gsub!(" \?").each_with_index { |v, i| " #{conditions[i + 1]}" } if conditions[0][" \?"]
      conditions = [where_chain( conditions[0] )]
    else
      conditions = nil
    end

    unless conditions.blank?
      @params[:filter]          ||= { type: "and", fields: [] }
      @params[:filter][:fields]  += conditions
    end
    self
  end

  def order(*columns)
    columns = columns[0] if columns[0].is_a?(Hash) || columns[0].is_a?(Array)
    
    if @params[:queryType] != GroupBy
      @params[:metric]   ||= []
      @params[:metric]    += columns.map{|column, direction| column }
      @params[:descending] = columns.any?{|column, direction| direction.to_s[/desc/]}
    else
      @params[:limitSpec]         ||= {}
      @params[:limitSpec][:type]  ||= "default"
      @params[:limitSpec][:limit] ||= 500000
      @params[:limitSpec][:columns] = columns.map{|column, direction| 
        {
          dimension: column.to_s,
          direction: direction.to_s[/desc/] ? "descending" : "ascending",
          dimensionOrder: "lexicographic"
        }
      }
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
    @params[:queryType] = TopN
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
    params = params.slice(*Permit_Properties[params[:queryType]])
    ap params if ENV['DEBUG']
    puts params.to_json if ENV['DEBUG']
    result = HTTParty.post(@url, body: params.to_json, headers: { 'Content-Type' => 'application/json' })
    puts result.code if ENV['DEBUG']

    # The result is a String, try to find the existence of substring 'pagingIdentifiers'.
    if page_count && result["pagingIdentifiers"]
      params.delete(:pagingSpec)
      current = params.hash

      # The pagingIdentifiers is something like { "publisher_daily_report_2017-03-01T00:00:00.000Z_2017-03-11T00:00:00.000Z_2017-04-17T21:04:30.804Z" => -10 }
      @paging_identifiers[current]            ||= {}
      @paging_identifiers[current][page_count]  = JSON.parse(result.body)[0]["result"]["pagingIdentifiers"].transform_values{|value| value + 1}
    end
    # ap JSON.parse(result) if ENV['DEBUG']
    
    result.body
  end

  def time_boundary
    @params[:queryType] = TimeBoundary
    self
  end

  def max_time
    @params[:queryType] = TimeBoundary
    @params[:bound]     = "maxTime"
    self
  end

  def min_time
    @params[:queryType] = TimeBoundary
    @params[:bound]     = "minTime"
    self
  end

  def source_meta
    @params[:queryType] = DataSourceMetaData
    self
  end

  def segment_meta
    @params[:queryType] = SegmentMetaData
    self
  end

  def to_s
    query
  end

  def to_a
    result = JSON.parse(query)
    @params[:queryType] == SegmentMetaData ? result[0]["columns"] : begin result[0]["result"]["events"] rescue result end
  end

  def each(&block)
    to_a.each(&block)
  end

  def map(&block)
    to_a.map(&block)
  end

  def get
    result = HTTParty.get(@url)
    puts result.code if ENV["DEBUG"]
    result.body
  end

  def delete
    result = HTTParty.delete(@url)
    puts result.code if ENV["DEBUG"]
    result.body
  end

  private

  def where_chain(conditions)
    conditions = conditions[1..-2] while conditions[0] == "\(" && conditions[-1] == "\)"

    if conditions[/ (or|and) /]
      %w(or and).each do |relation|
        mark = " #{relation} "
        if conditions[mark]
          parts = conditions.split(mark)
          return { type: relation, fields: parts.map{|part| where_chain(part)} } if check_brackets(parts)
          
          (parts.length - 2).downto(0) do |i|
            left  = parts[0  .. i].join(mark)
            right = parts[i+1..-1].join(mark)
            return { type: relation, fields: [where_chain(left), where_chain(right)] } if check_brackets(left) && check_brackets(right)
          end
        end
      end
    else
      column, op, value = conditions.split(/ (\<|\>|\<\=|\>\=|\=|\~|regex|in) /).map(&:strip)
      case op
      when "="  then { type: "selector", dimension: column, value: value }
      when ">"  then { type: "bound", dimension: column, lower: value, ordering: "numeric" }
      when ">=" then { type: "bound", dimension: column, lower: value, ordering: "numeric", lowerStrict: false }
      when "<"  then { type: "bound", dimension: column, upper: value, ordering: "numeric" }
      when "<=" then { type: "bound", dimension: column, upper: value, ordering: "numeric", upperStrict: false }
      when "~"  then value = JSON.parse(value); { type: "bound", dimension: column, lower: value[0], upper: value[1], ordering: "numeric"}
      when "regex" then value.gsub!(/[\"\']/,""); { type: "regex", dimension: column, pattern: value }
      when "in" then { type: "in", dimension: column, values: JSON.parse(values) }
      else nil
      end
    end
  end
  
  def having_chain(conditions)
    conditions = conditions[1..-2] while conditions[0] == "\(" && conditions[-1] == "\)"
    
    if conditions[/ (or|and) /]
      %w(or and).each do |relation|
        mark = " #{relation} "
        if conditions[mark]
          parts = conditions.split(mark)
          return { type: relation, havingSpecs: parts.map{|part| having_chain(part)} } if check_brackets(parts)
          
          (parts.length - 2).downto(0) do |i|
            left  = parts[0  .. i].join(mark)
            right = parts[i+1..-1].join(mark)
            return { type: relation, havingSpecs: [having_chain(left), having_chain(right)] } if check_brackets(left) && check_brackets(right)
          end
        end
      end
    elsif conditions[/[\<\>\=]/]
      column, op, value = conditions.split(/( [\<\>\=] )/).map(&:strip)
      { type: OPERATIONS[op], aggregation: column, value: value.to_f }
    else
      nil
    end
  end

  def post_chain(sentences)
    sentences, naming  = sentences.split(" as ")
    sentences = sentences[1..-2] while sentences[0] == "\(" && sentences[-2..-1] == "\)\)"
    
    if sentences[/( (\+\+|\-\-|\*\*|\/\/) )/]
      %w(+ - * /).each do |op|
        mark = " #{op*2} "
        if sentences[mark]
          parts = sentences.split(mark)

          (parts.length - 2).downto(0) do |i|
            left  = parts[0  .. i].join(mark)
            right = parts[i+1..-1].join(mark)
            return { type: "arithmetic", name: naming, fn: op, fields: [post_chain(left), post_chain(right)] } if check_brackets(left) && check_brackets(right)
          end
        end
      end
    else
      method_column(sentences)

      { type: "fieldAccess", name: naming, fieldName: sentences }
    end
  end

  def select_lookup(columns)
    if columns.present?
      @params[:dimensions] ||= []
      @params[:dimensions]  += columns.map{|columns|
        {
          type:       "lookup",
          dimension:  columns[:dimension] || columns["dimension"],
          outputName: columns[:output] || columns["output"],
          name:       columns[:name] || columns["name"],
          retainMissingValue: true,
        }
      }
    end
  end

  def method_column(column)
    method = column[/(sum|max|min|count)/i].downcase
    column = column.split(" as ")[0].gsub(/#{method}/i,"").gsub(/[\(\)]/,"")

    # Add the column to aggregations, which name is like sum(column), min(column), max(column), count(column)
    send(method, column)
  end

  def check_brackets(*sentences)
    sentences.flatten!
    sentences.all?{|sentence| sentence.scan("\(").count == sentence.scan("\)").count }
  end

end
