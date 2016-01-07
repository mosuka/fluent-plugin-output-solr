# Fluent::Plugin::OutSolr

This is a [Fluentd](http://fluentd.org/) output plugin for send data to [Apache Solr](http://lucene.apache.org/solr/). It support [SolrCloud](https://cwiki.apache.org/confluence/display/solr/SolrCloud) not only Standalone Solr.

## Installation

Add this line to your application's Gemfile:

```
gem 'fluent-plugin-output-solr'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-output-solr

## Examples

### Sent to standalone Solr
```
<match something.logs>
  @type solr

  url http://localhost:8983/solr/collection1

  batch_size 100

  buffer_type memory
  buffer_queue_limit 64m
  buffer_chunk_limit 8m
  flush_interval 10s
</match>
```

### Sent to SolrCloud
```
<match something.logs>
  @type solr

  zk_host localhost:2181/solr
  collection collection1

  batch_size 100

  buffer_type memory
  buffer_queue_limit 64m
  buffer_chunk_limit 8m
  flush_interval 10s
</match>
```

## Development

After checking out the repo, run `bundle install` to install dependencies. Then, run `rake test` to run the tests.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mosuka/fluent-plugin-output-solr.

