require 'helper'

class SolrOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @zk_server = nil
  end

  CONFIG_CONFIGURE = %[
    url                           http://localhost:8983/solr/collection1
    zk_host                       localhost:3292/solr
    collection                    collection1
    defined_fields                ["id", "title"]
    ignore_undefined_fields       true
    string_field_value_max_length -1
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  CONFIG_STANDALONE = %[
    url                           http://localhost:8983/solr/collection1
    defined_fields                ["id", "title"]
    ignore_undefined_fields       true
    string_field_value_max_length -1
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  CONFIG_SOLRCLOUD = %[
    zk_host                       localhost:3292/solr
    collection                    collection1
    defined_fields                ["id", "title"]
    ignore_undefined_fields       true
    string_field_value_max_length -1
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  CONFIG_SCHEMALESS = %[
    url                           http://localhost:8983/solr/collection1
    ignore_undefined_fields       false
    string_field_value_max_length -1
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  CONFIG_DEFINE_SCHEMA = %[
    url                           http://localhost:8983/solr/collection1
    defined_fields                ["id", "title"]
    ignore_undefined_fields       true
    string_field_value_max_length -1
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  CONFIG_STRING_FIELD_MAX_LENGTH = %[
    url                           http://localhost:8983/solr/collection1
    defined_fields                ["id", "title"]
    ignore_undefined_fields       true
    string_field_value_max_length 5
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%dT%H:%M:%S %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  CONFIG_TIME_FORMAT = %[
    url                           http://localhost:8983/solr/collection1
    ignore_undefined_fields       false
    string_field_value_max_length -1
    unique_key_field              id
    tag_field                     tag
    time_field                    time
    time_format                   %Y-%m-%d %H:%M:%S.%L %Z
    time_output_format            %FT%TZ
    flush_size                    100
    commit_with_flush             true
  ]

  def create_driver(conf = CONFIG_STANDALONE, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::SolrOutput, tag).configure(conf)
  end

  def sample_record
    {'id' => 'change.me', 'title' => 'change.me'}
  end

  def sample_multivalued_record
    {'id' => 'change.me', 'title' => ['change.me 1', 'change.me 2']}
  end

  def sample_reserved_data
    {'id' => 'change.me', 'title' => 'change.me', '_version_' => 123456}
  end

  def sample_time
    {'id' => 'change.me', 'title' => 'change.me', 'time' => '2016-01-01 09:00:00 UTC'}
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
    assert_equal 'http://localhost:8983/solr/collection1', d.instance.url
    assert_equal 'localhost:3292/solr', d.instance.zk_host
    assert_equal 'collection1', d.instance.collection
    assert_equal ['id', 'title'], d.instance.defined_fields
    assert_equal true, d.instance.ignore_undefined_fields
    assert_equal -1, d.instance.string_field_value_max_length
    assert_equal 'id', d.instance.unique_key_field
    assert_equal 'tag', d.instance.tag_field
    assert_equal 'time', d.instance.time_field
    assert_equal '%Y-%m-%dT%H:%M:%S %Z', d.instance.time_format
    assert_equal '%FT%TZ', d.instance.time_output_format
    assert_equal 100, d.instance.flush_size
    assert_equal true, d.instance.commit_with_flush
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
    start_zookeeper

    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SOLRCLOUD
    d.emit(sample_record, time)
    d.expect_format "\x93\xA4test\xCEV\x86@\x10\x82\xA2id\xA9change.me\xA5title\xA9change.me".force_encoding("ascii-8bit")
    d.run

    stop_zookeeper
  end

  def test_write_standalone
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STANDALONE

    d.emit(sample_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field></doc></add>', @index_cmds)
  end

  def test_write_solrcloud
    start_zookeeper

    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SOLRCLOUD

    d.emit(sample_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field></doc></add>', @index_cmds)

    stop_zookeeper  
  end

  def test_write_multivalued
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STANDALONE

    d.emit(sample_multivalued_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me 1</field><field name="title">change.me 2</field></doc></add>', @index_cmds)
  end

  def test_schemaless
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SCHEMALESS

    d.emit(sample_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="tag">test</field><field name="time">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)
  end

  def test_write_string_field_max_length
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_STRING_FIELD_MAX_LENGTH

    d.emit(sample_record, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">chang</field><field name="title">chang</field></doc></add>', @index_cmds)
  end

  def test_reserved_data
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_SCHEMALESS

    d.emit(sample_reserved_data, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="tag">test</field><field name="time">2016-01-01T09:00:00Z</field></doc></add>', @index_cmds)
  end

  def test_time_format
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    stub_solr_update 'http://localhost:8983/solr/collection1/update?commit=true&wt=ruby'

    d = create_driver CONFIG_TIME_FORMAT

    d.emit(sample_time, time)
    d.run

    assert_equal('<?xml version="1.0" encoding="UTF-8"?><add><doc><field name="id">change.me</field><field name="title">change.me</field><field name="time">2016-01-01T09:00:00Z</field><field name="tag">test</field></doc></add>', @index_cmds)
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