require 'helper'

class SolrOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG_STANDALONE = %[
    url                     http://localhost:8983/solr/collection1
    defined_fields          ["id", "title"]
    ignore_undefined_field  true
    unique_key_field        id
    timestamp_field         event_timestamp
    flush_size              100
  ]

  CONFIG_SOLRCLOUD = %[
    zk_host                 localhost:3292/solr
    collection              collection1
    defined_fields          ["id", "title"]
    ignore_undefined_field  true
    unique_key_field        id
    timestamp_field         event_timestamp
    flush_size              100
  ]

  def create_driver(conf = CONFIG_STANDALONE, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::SolrOutput, tag).configure(conf)
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
    assert_equal 100, d.instance.flush_size
  end

  def test_configure_solrcloud
    d = create_driver CONFIG_SOLRCLOUD
    assert_equal 'localhost:3292/solr', d.instance.zk_host
    assert_equal 'collection1', d.instance.collection
    assert_equal 100, d.instance.flush_size
  end

  def test_format_standalone
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STANDALONE
    d.emit(sample_record, time)
    d.expect_format "\x93\xA4test\xCEV\x86@\x10\x82\xA2id\xA9change.me\xA5title\xA9change.me".force_encoding("ascii-8bit")
    d.run
  end

  def test_format_solrcloud
    server = ZK::Server.new do |config|
      config.client_port = 3292
      config.enable_jmx = true
      config.force_sync = false
    end

    server.run

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
    
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SOLRCLOUD
    d.emit(sample_record, time)
    d.expect_format "\x93\xA4test\xCEV\x86@\x10\x82\xA2id\xA9change.me\xA5title\xA9change.me".force_encoding("ascii-8bit")
    d.run

    server.shutdown
  end

  def test_write_standalone
    d = create_driver

    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STANDALONE

    d.instance.unique_key_field = 'id'
    d.instance.defined_fields = ['id', 'title']

    d.emit(sample_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="event_timestamp">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)
  end

  def test_write_solrcloud
    d = create_driver

    server = ZK::Server.new do |config|
      config.client_port = 3292
      config.enable_jmx = true
      config.force_sync = false
    end

    server.run

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
    
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SOLRCLOUD

    d.instance.unique_key_field = 'id'
    d.instance.defined_fields = ['id', 'title']

    d.emit(sample_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="event_timestamp">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)

    server.shutdown    
  end
end