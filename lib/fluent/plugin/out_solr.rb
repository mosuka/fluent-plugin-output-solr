require 'securerandom'
require 'rsolr'
require 'zk'
require 'rsolr/cloud'

module Fluent
  class SolrOutput < BufferedOutput
    Fluent::Plugin.register_output('out_solr', self)

    config_param  :mode, :string, :default => 'Standalone',
                  :desc => 'The olr server mode, it can be Standalone or SolrCloud.'

    config_param  :url, :string, :default => 'http://localhost:8983/solr/collection1',
                  :desc => 'The Solr server url.'

    config_param :zk_host, :string, :default => 'localhost:2181/solr',
                  :desc => 'The ZooKeeper connection string that SolrCloud refers to.'
    config_param :collection, :string, :default => 'collection1',
                  :desc => 'The SolrCloud collection name.'

    config_param :batch_size, :integer, :default => 100,
                  :desc => 'The batch size used in update.'

    def initialize
      super
    end

    def configure(conf)
      super

      @mode = conf['mode']

      @url = conf['url']

      @zk_host = conf['zk_host']
      @collection = conf['collection']

      @batch_size = conf['batch_size'].to_i
    end

    def start
      super

      if @mode == 'Standalone' then
        @solr = RSolr.connect :url => @url
      elsif @mode == 'SolrCloud' then
        @zk = ZK.new(@zk_host)
        @cloud_connection = RSolr::Cloud::Connection.new(@zk)
        @solr = RSolr::Client.new(@cloud_connection, read_timeout: 60, open_timeout: 60)
      else
        raise 'Unexpected mode specified.'
      end
    end

    def shutdown
      super

      if @mode == 'SolrCloud' then
        @zk.close
      end
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
      #[tag, time, record].to_json
    end

    def write(chunk)
      documents = []

      chunk.msgpack_each do |tag, time, record|
        record.merge!({'id' => SecureRandom.uuid})

        documents << record
      
        if documents.count >= @batch_size
          if @mode == 'Standalone' then
            @solr.add documents
            log.info 'Sent a commit to Solr.'
            @solr.commit
            log.info "Added %d document(s) to Solr" % documents.count
          elsif @mode == 'SolrCloud' then
            @solr.add documents, collection: @collection
            log.info 'Sent a commit to Solr.'
            @solr.commit collection: @collection
            log.info "Added %d document(s) to Solr" % documents.count
          else
            raise 'Unexpected mode specified.'
          end

          documents.clear
        end
      end
      
      if documents.count > 0 then
        if @mode == 'Standalone' then
          @solr.add documents
          log.info 'Sent a commit to Solr.'
          @solr.commit
          log.info "Added %d document(s) to Solr" % documents.count
        elsif @mode == 'SolrCloud' then
          @solr.add documents, collection: @collection
          log.info 'Sent a commit to Solr.'
          @solr.commit collection: @collection
          log.info "Added %d document(s) to Solr" % documents.count
        else
          raise 'Unexpected mode specified.'
        end

        documents.clear
      end
    end
  end
end
