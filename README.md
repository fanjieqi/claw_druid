# claw_druid

A Ruby client for [Druid](http://druid.io).

## Installation

Add following line to Gemfile:

```
gem 'claw_druid', '~> 0.0.3'
```

Install it directly:

```
gem install claw_druid -v 0.0.3
```

## Initization

```ruby
client = ClawDruid.new(url: 'http://druid_broker.com:port/druid/v2/', source: 'table_name')
```

## select
The complex 'sum', 'max', 'min' and 'count' with operators will be transformed into [Post-Aggregations](http://druid.io/docs/0.10.0/querying/post-aggregations.html) automatically.
The select of [lookups](http://druid.io/docs/latest/querying/lookups.html) shoud be a hash.
```ruby
client.select(:dimension1, :dimension2, :column1, :column2)
client.select("sum(column1) as sum_column1, sum(column2)")
client.select("sum(column1 + column2) as col1_col2")
client.select("(sum(column1 + column2) // sum(column3)) as col1_col2_col3")

client.select({dimension: :dimension_id, output: :dimension_name, name: :lookup_names})
```

## where
Add the conditions to [filters](http://druid.io/docs/0.10.0/querying/filters.html).
```ruby
client.where(dimension1: v1, dimension2: v2)
client.where("dimension1 = #{v1} and dimension2 > #{v2}")
client.where("dimension1 >= ? or dimension2 <= ?", v1, v2)
client.where(dimension1: [v1, v2, v3]) # dimension1 in (v1, v2, v4)
```

## group
Add the dimensions to [dimensions](http://druid.io/docs/0.10.0/querying/dimensionspecs.html) by [groupBy](http://druid.io/docs/0.10.0/querying/groupbyquery.html).
The groupby of [lookups](http://druid.io/docs/latest/querying/lookups.html) shoud be a hash.
```ruby
client.group(:dimension1)
client.group(:dimension1, :dimension2)

client.group({dimension: :game_cd, output: :game_name, name: :lookup_names})
```

## having
Add the having conditions to [having](http://druid.io/docs/0.10.0/querying/having.html).
```
client.group(:dimension1).having("sum(column1) > #{v1}")
client.group(:dimension1, :dimension2).having("sum(column1) > ? and sum(column2) <= ?", v1, v2)
```

## sum
Get the [sum](http://druid.io/docs/0.10.0/querying/aggregations.html#sum-aggregators) result by using methods in [Aggregations](http://druid.io/docs/0.10.0/querying/aggregations.html)
```ruby
client.where(dimension1: v1, dimension2: v2).sum(:column1)
client.where(dimension1: v1, dimension2: v2).sum(:column1, :column2)

client.group(:dimension1, :dimension2).sum(:column1)
client.group(:dimension1, :dimension2).sum(:column1, :column2)

client.group(:dimension1, :dimension2).having("sum(column1) > ? and sum(column2) <= ?", v1, v2).sum(:column1, :column2)
```

## max, min
Get the [max min](http://druid.io/docs/0.10.0/querying/aggregations.html#min-max-aggregators) result by using methods in [Aggregations](http://druid.io/docs/0.10.0/querying/aggregations.html)
```ruby
client.where(dimension1: v1, dimension2: v2).max(:column1)
client.where(dimension1: v1, dimension2: v2).min(:column1, :column2)

client.group(:dimension1, :dimension2).min(:column1)
client.group(:dimension1, :dimension2).max(:column1, :column2)

client.group(:dimension1, :dimension2).having("max(column1) > ? and min(column2) <= ?", v1, v2).max(:column1).min(:column2)
```

## count
Get the [count](http://druid.io/docs/0.10.0/querying/aggregations.html#count-aggregator).
```ruby
client.where(dimension1: v1, dimension2: v2).count

client.group(:dimension1, :dimension2).count
client.group(:dimension1, :dimension2).having("min(column1) > ? and min(column2) <= ?", v1, v2).count
```

## count cardinality
Count [cardinality](http://druid.io/docs/0.10.0/querying/aggregations.html#cardinality-aggregator) of columns.
```ruby
client.where(dimension1: v1, dimension2: v2).count(:column1)
client.where(dimension1: v1, dimension2: v2).count(:column1, :column2)

client.group(:dimension1, :dimension2).count(:column1)
client.group(:dimension1, :dimension2).count(:column1, :column2)

client.group(:dimension1, :dimension2).having("min(column1) > ? and min(column2) <= ?", v1, v2).count(:column1, :column2)
```

## limit
Get the limited results by using [limit](http://druid.io/docs/latest/querying/limitspec.html).
```ruby
client.where(dimension1: v1, dimension2: v2).select(:dimension1, :dimension2).limit(100)
```

## topN
Get the top results by using [topN](http://druid.io/docs/latest/querying/topnquery.html).
```ruby
client.where(dimension1: v1, dimension2: v2).select(:dimension1, :dimension2).top(100)
```

## order
Get the orderd results by using [order](http://druid.io/docs/latest/querying/sorting-orders.html).
```ruby
client.where(dimension1: v1, dimension2: v2).select(:dimension1, :dimension2).order(:dimension1, :dimension2)
client.where(dimension1: v1, dimension2: v2).select(:dimension1, :dimension2).order(dimension1: :desc)
client.where(dimension1: v1, dimension2: v2).select(:dimension1, :dimension2).order(dimension1: :desc, :dimension2)
```

## Intervals
Set the intervals by adding begin_time and end_time to where conditions.
```ruby
client.where(begin_time: time1, end_time: time2)
```

## query
Get the result.
```ruby
client.where().group().sum().query
```

## [time_boundary](http://druid.io/docs/latest/querying/timeboundaryquery.html)
```ruby
client.time_boundary
client.max_time
client.min_time
```

## [source_meta](http://druid.io/docs/latest/querying/datasourcemetadataquery.html)
```ruby
client.source_meta
```

## [segment_meta](http://druid.io/docs/latest/querying/segmentmetadataquery.html)
```ruby
client.segment_meta
```

## Enumerable
```ruby
records = client.where().group().sum()
result  = records.to_a
records.map do |record| 
  # value
end
records.each do |record|
  # do something
end
```
