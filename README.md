# Fluent::Plugin::SolrOutput

This is a [Fluentd](http://fluentd.org/) output plugin for send data to [Apache Solr](http://lucene.apache.org/solr/). It support [SolrCloud](https://cwiki.apache.org/confluence/display/solr/SolrCloud) not only Standalone Solr.

## Requirements

| fluent-plugin-output-solr | fluentd         | td-agent | ruby   |
| ------------------------- | --------------- | -------- | ------ |
| 1.x.x                     | >= 0.14.0, < 2  | 3        | >= 2.1 |
| 0.x.x                     | ~> 0.12.0       | 2        | >= 1.9 |

* The 1.x.x series is developed from this branch (master)
* The 0.x.x series (compatible with fluentd v0.12, and td-agent 2) is developed on the [v0.x.x branch](https://github.com/mosuka/fluent-plugin-output-solr/tree/v0.x.x)

## Installation

Install it yourself as:

```
$ gem install fluent-plugin-output-solr
```

## How to build

```
$ gem install bundler
$ bundle install
$ rake test
$ rake build
$ rake install
```

## Config parameters

### base_url

The Solr base url (for example http://localhost:8983/solr).

```
base_url http://localhost:8983/solr
```

### zk_host

The ZooKeeper connection string that SolrCloud refers to (for example localhost:2181/solr).

```
zk_host localhost:2181/solr
```

### collection

The Solr collection/core name (default collection1).

```
collection collection1
```

### ignore_undefined_fields

Ignore undefined fields in the Solr schema.xml.

```
ignore_undefined_fields false
```

### tag_field

A field name of fluentd tag in the Solr schema.xml (default tag).

```
tag_field tag
```

### time_field

A field name of event timestamp in the Solr schema.xml (default time).

```
time_field time
```

### time_format

The format of the time field (default %FT%TZ).

```
time_format %FT%TZ
```

### millisecond

Output millisecond to Solr (default false).

```
millisecond false
```

### flush_size

A number of events to queue up before writing to Solr (default 100).

```
flush_size 100
```

### commit_with_flush

Send commit command to Solr with flush (default true).

```
commit_with_flush true
```

## Plugin setup examples

### Sent to standalone Solr using data-driven schemaless mode.
```
<match something.logs>
  @type solr

  # The Solr base url (for example http://localhost:8983/solr).
  base_url http://localhost:8983/solr

  # The Solr collection/core name (default collection1).
  collection collection1
</match>
```

### Sent to SolrCloud using data-driven schemaless mode.
```
<match something.logs>
  @type solr

  # The ZooKeeper connection string that SolrCloud refers to (for example localhost:2181/solr).
  zk_host localhost:2181/solr

  # The Solr collection/core name (default collection1).
  collection collection1
</match>
```

## Development

After checking out the repo, run `bundle install` to install dependencies. Then, run `rake test` to run the tests.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mosuka/fluent-plugin-output-solr.
