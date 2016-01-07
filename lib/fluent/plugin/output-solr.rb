require 'securerandom'
require 'rsolr'
require 'zk'
require 'rsolr/cloud'

module Fluent
  class SolrOutput < BufferedOutput
    Fluent::Plugin.register_output('output-solr', self)

    config_param :url, :string, :default => nil,
                  :desc => 'The Solr server url (for example http://localhost:8983/solr/collection1).'

    config_param :zk_host, :string, :default => nil,
                  :desc => 'The ZooKeeper connection string that SolrCloud refers to (for example localhost:2181/solr).'
    config_param :collection, :string, :default => 'collection1',
                  :desc => 'The SolrCloud collection name.'

    config_param :batch_size, :integer, :default => 100,
                  :desc => 'The batch size used in update.'

    MODE_STANDALONE = 'Standalone'
    MODE_SOLRCLOUD = 'SolrCloud'

    def initialize
      super
    end

    def configure(conf)
      super

      @url = conf['url']

      @zk_host = conf['zk_host']
      @collection = conf['collection']

      @batch_size = conf['batch_size'].to_i
    end

    def start
      super

      @mode = nil
      if ! @url.nil? then
        @mode = MODE_STANDALONE
      elsif ! @zk_host.nil?
        @mode = MODE_SOLRCLOUD
      end

      @solr = nil
      @zk = nil

      if @mode == MODE_STANDALONE then
        @solr = RSolr.connect :url => @url
      elsif @mode == MODE_SOLRCLOUD then
        @zk = ZK.new(@zk_host)
        cloud_connection = RSolr::Cloud::Connection.new(@zk)
        @solr = RSolr::Client.new(cloud_connection, read_timeout: 60, open_timeout: 60)
      end
    end

    def shutdown
      super

      unless @zk.nil? then
        @zk.close
      end
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      documents = []

      chunk.msgpack_each do |tag, time, record|
        unless record.has_key?('id') then
          record.merge!({'id' => SecureRandom.uuid})
        end

        documents << record
      
        if documents.count >= @batch_size
          update documents
          documents.clear
        end
      end
      
      update documents unless documents.empty?
    end

    def update(documents)
      if @mode == MODE_STANDALONE then
        @solr.add documents, :params => {:commit => true}
        log.info "Added %d document(s) to Solr" % documents.count
      elsif @mode == MODE_SOLRCLOUD then
        @solr.add documents, collection: @collection, :params => {:commit => true}
        log.info "Added %d document(s) to Solr" % documents.count
      end
    end
  end
end
