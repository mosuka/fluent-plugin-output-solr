require 'helper'

class SolrOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
    @zk_server = nil
  end

  CONFIG_CONFIGURE = '
    base_url                      http://localhost:8983/solr
    zk_host                       localhost:3292/solr
    collection                    collection1
    ignore_undefined_fields       false
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    millisecond                   false
    flush_size                    100
    commit_with_flush             true
  '

  CONFIG_STANDALONE = '
    base_url                      http://localhost:8983/solr
    collection                    collection1
    ignore_undefined_fields       false
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    millisecond                   false
    flush_size                    100
    commit_with_flush             true
  '

  CONFIG_SOLRCLOUD = '
    zk_host                       localhost:3292/solr
    collection                    collection1
    ignore_undefined_fields       false
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    millisecond                   false
    flush_size                    100
    commit_with_flush             true
  '

  CONFIG_TIME_FORMAT = '
    base_url                      http://localhost:8983/solr
    collection                    collection1
    ignore_undefined_fields       false
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%d %H:%M:%S.%L %Z
    millisecond                   true
    flush_size                    100
    commit_with_flush             true
  '

  def create_driver(conf = CONFIG_STANDALONE)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::SolrOutput).configure(conf)
  end

  def stub_solr_update(url = 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby')
    stub_request(:post, url).with do |req|
      @index_cmds = req.body
    end
  end

  def stub_solr_unique_key(url = 'http://localhost:8983/solr/collection1/schema/uniquekey')
    stub_request(:post, url).with do |req|
      @index_cmds = req.body
    end
  end

  def delete_nodes(zk, path)
    zk.children(path).each do |node|
      delete_nodes(zk, File.join(path, node))
    end
    zk.delete(path)
  rescue ZK::Exceptions::NoNode
  end

  def create_nodes(zk, path)
    parent_path = File.dirname(path)
    unless zk.exists?(parent_path, :watch => true) then
      create_nodes(zk, parent_path)
    end
    zk.create(path)
  end

  def test_configure
    d = create_driver CONFIG_CONFIGURE
    assert_equal 'http://localhost:8983/solr', d.instance.base_url
    assert_equal 'localhost:3292/solr', d.instance.zk_host
    assert_equal 'collection1', d.instance.collection
    assert_equal 'tag', d.instance.tag_field
    assert_equal 'time', d.instance.time_field
    assert_equal '%Y-%m-%dT%H:%M:%S %Z', d.instance.time_format
    assert_equal false, d.instance.millisecond
    assert_equal 100, d.instance.flush_size
    assert_equal true, d.instance.commit_with_flush
  end

  def test_invalid_chunk_keys
    assert_raise_message(/'tag' in chunk_keys is required./) do
      create_driver(Fluent::Config::Element.new(
          'ROOT', '', {
          '@type'                   => 'solr',
          'base_url'                => 'http://localhost:8983/solr',
          'collection'              => 'collection1',
          'ignore_undefined_fields' => true,
          'tag_field'               => 'tag',
          'time_field'              => 'time',
          'flush_size'              => 100,
          'commit_with_flush'       => true
      }, [
              Fluent::Config::Element.new('buffer', 'mykey', {
                  'chunk_keys' => 'mykey'
              }, [])
          ]))
    end
  end

  def test_format_standalone
    time = event_time('2016-01-01 09:00:00 UTC')

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    sample_record = {'id' => 'change.me', 'title' => 'change.me'}

    d = create_driver CONFIG_STANDALONE
    d.run(default_tag: "test") do
      d.feed(time, sample_record)
    end

    assert_equal [time, sample_record].to_msgpack, d.formatted[0], d.formatted[0]
  end

  def test_format_solrcloud
    start_zookeeper

    time = event_time('2016-01-01 09:00:00 UTC')

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    sample_record = {'id' => 'change.me', 'title' => 'change.me'}

    d = create_driver CONFIG_SOLRCLOUD
    d.run(default_tag: "test") do
      d.feed(time, sample_record)
    end

    assert_equal [time, sample_record].to_msgpack, d.formatted[0]

    stop_zookeeper
  end

  def test_write_standalone
    time = event_time('2016-01-01 09:00:00 UTC')

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    sample_record = {'id' => 'change.me', 'title' => 'change.me'}

    d = create_driver CONFIG_STANDALONE
    d.run(default_tag: 'test') do
      d.feed(time, sample_record)
    end

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="tag">test</field><field name="time">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)
  end

  def test_write_solrcloud
    start_zookeeper

    time = event_time('2016-01-01 09:00:00 UTC')

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    sample_record = {'id' => 'change.me', 'title' => 'change.me'}

    d = create_driver CONFIG_SOLRCLOUD
    d.run(default_tag: 'test') do
      d.feed(time, sample_record)
    end

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="tag">test</field><field name="time">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)

    stop_zookeeper
  end

  def test_write_multivalued
    time = event_time('2016-01-01 09:00:00 UTC')

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    sample_multivalued_record = {'id' => 'change.me', 'title' => ['change.me 1', 'change.me 2']}

    d = create_driver CONFIG_STANDALONE
    d.run(default_tag: 'test') do
      d.feed(time, sample_multivalued_record)
    end

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me 1</field><field name="title">change.me 2</field><field name="tag">test</field><field name="time">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)
  end

  def test_time_format
    time = event_time('2016-01-01 09:00:00 UTC')

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    sample_time = {'id' => 'change.me', 'title' => 'change.me', 'time' => '2016-01-01 09:00:00.123 UTC'}

    d = create_driver CONFIG_TIME_FORMAT
    d.run(default_tag: 'test') do
      d.feed(time, sample_time)
    end

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="time">2016-01-01T09:00:00.123Z</field><field name="tag">test</field></doc></add>', @index_cmds)
  end

  def start_zookeeper
    @zk_server = ZK::Server.new do |config|
      config.client_port = 3292
      config.enable_jmx = true
      config.force_sync = false
    end

    @zk_server.run

    zk = ZK.new('localhost:3292')
    delete_nodes(zk, '/solr')
    create_nodes(zk, '/solr/live_nodes')
    create_nodes(zk, '/solr/collections')
    ['localhost:8983_solr'].each do |node|
      zk.create("/solr/live_nodes/#{node}", '', mode: :ephemeral)
    end
    ['collection1'].each do |collection|
      zk.create("/solr/collections/#{collection}")
      json = File.read("test/files/collections/#{collection}/state.json")
      zk.create("/solr/collections/#{collection}/state.json", json, mode: :ephemeral)
    end
  end

  def stop_zookeeper
    @zk_server.shutdown
  end
end
