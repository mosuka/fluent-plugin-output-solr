require 'securerandom'
require 'rsolr'
require 'zk'
require 'rsolr/cloud'

module Fluent
  class SolrOutput < BufferedOutput
    Fluent::Plugin.register_output('out_solr', self)

    config_param  :mode, :string, :default => 'Standalone',
                  :desc => 'The olr server mode, it can be Standalone or SolrCloud.'

    config_param  :solr_url, :string, :default => 'http://localhost:8983/solr',
                  :desc => 'The Solr server url.'
    config_param :solr_core, :string, :default => 'collection1',
                  :desc => 'The Solr core name.'

    config_param :solrcloud_zkhost, :string, :default => 'localhost:2181/solr',
                  :desc => 'The ZooKeeper connection string that SolrCloud refers to.'
    config_param :solrcloud_collection, :string, :default => 'collection1',
                  :desc => 'The SolrCloud collection name.'

    config_param :batch_size, :integer, :default => 100,
                  :desc => 'The batch size used in update.'

    def initialize
      super
    end

    def configure(conf)
      super

      @mode = conf['mode']

      @batch_size = conf['batch_size'].to_i

      @solr_url = conf['solr_url']
      @solr_core = conf['solr_core']

      @solrcloud_zkhost = conf['solrcloud_zkhost']
      @solrcloud_collection = conf['solrcloud_collection']
    end

    def start
      super

      if @mode == 'Standalone' then
        @solr = RSolr.connect :url => @solr_url.end_with?('/') ? @solr_url + @solr_core : @solr_url + '/' + @solr_core
      elsif @mode == 'SolrCloud' then
        @solr = RSolr::Client.new(RSolr::Cloud::Connection.new(ZK.new(@solrcloud_zkhost)), read_timeout: 60, open_timeout: 60)
      else
        raise 'Unexpected mode specified.'
      end
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      documents = []

      chunk.msgpack_each do |tag, time, record|
        record.merge!({'id' => SecureRandom.uuid})

        documents << record
      
        if documents.count >= @batch_size
          @solr.add documents
          log.info "Added %d document(s) to Solr" % documents.count
          @solr.commit
          log.info 'Sent a commit to Solr.'
          documents.clear
        end
      end
      
      if documents.count > 0 then
        @solr.add documents
        log.info "Added %d document(s) to Solr" % documents.count
        @solr.commit
        log.info 'Sent a commit to Solr.'
        documents.clear
      end
    end
  end
end