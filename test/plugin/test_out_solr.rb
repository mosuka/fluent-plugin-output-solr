require 'helper'
require 'securerandom'

class SolrOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG_STANDALONE = %[
    mode                Standalone
    url                 http://localhost:8983/solr/collection1
    batch_size          100
  ]

  CONFIG_SOLRCLOUD = %[
    mode                SolrCloud
    zk_host             localhost:2181/solr
    collection          collection1
    batch_size          100
  ]

  def create_driver(conf = CONFIG_STANDALONE, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::SolrOutput, tag).configure(conf)
  end

  def sample_record
    {'id' => 'change.me', 'title' => 'change.me'}
  end

  def test_configure_standalone
    d = create_driver CONFIG_STANDALONE
    assert_equal 'Standalone', d.instance.mode
    assert_equal 'http://localhost:8983/solr/collection1', d.instance.url
    assert_equal 100, d.instance.batch_size

    d = create_driver CONFIG_SOLRCLOUD
    assert_equal 'SolrCloud', d.instance.mode
    assert_equal 'localhost:2181/solr', d.instance.zk_host
    assert_equal 100, d.instance.batch_size
  end

  def test_format
    time = Time.parse("2016-01-01 09:00:00 UTC").to_i

    d = create_driver CONFIG_STANDALONE
    d.emit(sample_record, time)
    d.expect_format "\x93\xA4test\xCEV\x86@\x10\x82\xA2id\xA9change.me\xA5title\xA9change.me".force_encoding("ascii-8bit")
    d.run

    # d = create_driver CONFIG_SOLRCLOUD
    # d.emit(sample_record, time)
    # d.expect_format "\x93\xA4test\xCEV\x86@\x10\x82\xA2id\xA9change.me\xA5title\xA9change.me".force_encoding("ascii-8bit")
    # d.run
  end

  def test_write
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # ### FileOutput#write returns path
    # path = d.run
    # expect_path = "#{TMP_DIR}/out_file_test._0.log.gz"
    # assert_equal expect_path, path
  end
end