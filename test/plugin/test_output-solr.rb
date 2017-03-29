require 'helper'

class SolrOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @zk_server = nil
  end

  CONFIG_STANDALONE = %[
    url                     http://localhost:8983/solr/collection1
    defined_fields          ["id", "title"]
    ignore_undefined_fields true
    unique_key_field        id
    tag_field               tag
    timestamp_field         event_timestamp
    flush_size              100
  ]

  CONFIG_SOLRCLOUD = %[
    zk_host                 localhost:3292/solr
    collection              collection1
    defined_fields          ["id", "title"]
    ignore_undefined_fields true
    unique_key_field        id
    tag_field               tag
    timestamp_field         event_timestamp
    flush_size              100
  ]

  def create_driver(conf = CONFIG_STANDALONE)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::SolrOutput).configure(conf)
  end

  def sample_record
    {'id' => 'change.me', 'title' => 'change.me'}
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

  def test_configure_standalone
    d = create_driver CONFIG_STANDALONE
    assert_equal 'http://localhost:8983/solr/collection1', d.instance.url
    assert_equal ['id', 'title'], d.instance.defined_fields
    assert_equal true, d.instance.ignore_undefined_fields
    assert_equal 'id', d.instance.unique_key_field
    assert_equal 'tag', d.instance.tag_field
    assert_equal 'event_timestamp', d.instance.timestamp_field
    assert_equal 100, d.instance.flush_size
  end

  def test_configure_solrcloud
    d = create_driver CONFIG_SOLRCLOUD
    assert_equal 'localhost:3292/solr', d.instance.zk_host
    assert_equal 'collection1', d.instance.collection
    assert_equal ['id', 'title'], d.instance.defined_fields
    assert_equal true, d.instance.ignore_undefined_fields
    assert_equal 'id', d.instance.unique_key_field
    assert_equal 'tag', d.instance.tag_field
    assert_equal 'event_timestamp', d.instance.timestamp_field
    assert_equal 100, d.instance.flush_size
  end

  def test_format_standalone
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STANDALONE
    d.run(default_tag: "test") do
      d.feed(time, sample_record)
    end
    assert_equal [time, sample_record].to_msgpack, d.formatted[0], d.formatted[0]
  end

  def test_format_solrcloud
    start_zookeeper

    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SOLRCLOUD
    d.run(default_tag: "test") do
      d.feed(time, sample_record)
    end
    assert_equal [time, sample_record].to_msgpack, d.formatted[0]

    stop_zookeeper
  end

  def test_write_standalone
    d = create_driver

    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STANDALONE

    #d.instance.unique_key_field = 'id'
    #d.instance.defined_fields = ['id', 'title']

    d.run(default_tag: "test") do
      d.feed(time, sample_record)
    end

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field></doc></add>', @index_cmds)
  end

  def test_write_solrcloud
    start_zookeeper

    d = create_driver

    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SOLRCLOUD

    #d.instance.unique_key_field = 'id'
    #d.instance.defined_fields = ['id', 'title']

    d.run(default_tag: "test") do
      d.feed(time, sample_record)
    end

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field></doc></add>', @index_cmds)

    stop_zookeeper
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
