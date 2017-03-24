require 'securerandom'
require 'rsolr'
require 'zk'
require 'rsolr/cloud'

module Fluent
  class SolrOutput < BufferedOutput
    Fluent::Plugin.register_output('solr', self)

    DEFAULT_COLLECTION = 'collection1'
    DEFAULT_IGNORE_UNDEFINED_FIELDS = false
    DEFAULT_TAG_FIELD = 'tag'
    DEFAULT_TIMESTAMP_FIELD = 'event_timestamp'
    DEFAULT_FLUSH_SIZE = 100

    MODE_STANDALONE = 'Standalone'
    MODE_SOLRCLOUD = 'SolrCloud'

    include Fluent::SetTagKeyMixin
    config_set_default :include_tag_key, false

    include Fluent::SetTimeKeyMixin
    config_set_default :include_time_key, false

    config_param :url, :string, :default => nil,
                 :desc => 'The Solr server url (for example http://localhost:8983/solr/collection1).'

    config_param :zk_host, :string, :default => nil,
                 :desc => 'The ZooKeeper connection string that SolrCloud refers to (for example localhost:2181/solr).'
    config_param :collection, :string, :default => DEFAULT_COLLECTION,
                 :desc => 'The SolrCloud collection name (default collection1).'

    config_param :defined_fields, :array, :default => nil,
                 :desc => 'The defined fields in the Solr schema.xml. If omitted, it will get fields via Solr Schema API.'                 
    config_param :ignore_undefined_fields, :bool, :default => DEFAULT_IGNORE_UNDEFINED_FIELDS,
                 :desc => 'Ignore undefined fields in the Solr schema.xml.'                 

    config_param :unique_key_field, :string, :default => nil,
                 :desc => 'A field name of unique key in the Solr schema.xml. If omitted, it will get unique key via Solr Schema API.'
    config_param :tag_field, :string, :default => DEFAULT_TAG_FIELD,
                 :desc => 'A field name of fluentd tag in the Solr schema.xml (default event_timestamp).'
    config_param :timestamp_field, :string, :default => DEFAULT_TIMESTAMP_FIELD,
                 :desc => 'A field name of event timestamp in the Solr schema.xml (default event_timestamp).'

    config_param :flush_size, :integer, :default => DEFAULT_FLUSH_SIZE,
                 :desc => 'A number of events to queue up before writing to Solr (default 100).'

    def initialize
      super
    end

    def configure(conf)
      super
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

      @fields = @defined_fields.nil? ? get_fields : @defined_fields
      @unique_key = @unique_key_field.nil? ? get_unique_key : @unique_key_field

      chunk.msgpack_each do |tag, time, record|
        unless record.has_key?(@unique_key) then
          record.merge!({@unique_key => SecureRandom.uuid})
        end

        unless record.has_key?(@tag_field) then
          record.merge!({@tag_field => tag})
        end

        if record.has_key?(@timestamp_field) then
          begin
            event_timestamp_dt = DateTime.strptime(record[@timestamp_field], "%d/%b/%Y:%H:%M:%S %z").to_s
            record.merge!({@timestamp_field => Time.parse(event_timestamp_dt.to_s).utc.strftime('%FT%TZ')})
          rescue
            record.merge!({@timestamp_field => Time.at(time).utc.strftime('%FT%TZ')})
          end
        else
          record.merge!({@timestamp_field => Time.at(time).utc.strftime('%FT%TZ')})
        end

        if @ignore_undefined_fields then
          record.each_key do |key|
            unless @fields.include?(key) then
              record.delete(key)
            end
          end
        end

        documents << record

        if documents.count >= @flush_size
          update documents
          documents.clear
        end
      end

      update documents unless documents.empty?
    end

    def update(documents)
      if @mode == MODE_STANDALONE then
        @solr.add documents, :params => {:commit => true}
        log.debug "Added %d document(s) to Solr" % documents.count
      elsif @mode == MODE_SOLRCLOUD then
        @solr.add documents, collection: @collection, :params => {:commit => true}
        log.debug "Update: Added %d document(s) to Solr" % documents.count
      end
      rescue Exception => e
        log.warn "Update: An error occurred while indexing: #{e.message}"
    end

    def get_unique_key
      response = nil

      if @mode == MODE_STANDALONE then
        response = @solr.get 'schema/uniquekey'
      elsif @mode == MODE_SOLRCLOUD then
        response = @solr.get 'schema/uniquekey', collection: @collection
      end

      unique_key = response['uniqueKey']
      log.debug "Unique key: #{unique_key}"

      return unique_key

      rescue Exception => e
        log.warn "Unique key: #{e.message}"
    end

    def get_fields
      response = nil

      if @mode == MODE_STANDALONE then
        response = @solr.get 'schema/fields'
      elsif @mode == MODE_SOLRCLOUD then
        response = @solr.get 'schema/fields', collection: @collection
      end

      fields = []
      response['fields'].each do |field|
        fields.push(field['name'])
      end
      log.debug "Fields: #{fields}"

      return fields

      rescue Exception => e
        log.warn "Fields: #{e.message}"
    end
  end
end
