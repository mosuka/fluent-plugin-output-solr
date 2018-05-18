require 'securerandom'
require 'rsolr'
require 'zk'
require 'rsolr/cloud'
require 'fluent/plugin/output'

module Fluent::Plugin
  class SolrOutput < Output
    Fluent::Plugin.register_output('solr', self)

    helpers :inject, :compat_parameters

    DEFAULT_COLLECTION = 'collection1'

    DEFAULT_TAG_FIELD = 'tag'

    DEFAULT_TIME_FIELD = 'time'
    DEFAULT_TIME_FORMAT = '%FT%TZ'
    DEFAULT_MILLISECOND = false

    DEFAULT_IGNORE_UNDEFINED_FIELDS = false

    DEFAULT_FLUSH_SIZE = 100
    DEFAULT_BUFFER_TYPE = 'memory'
    DEFAULT_COMMIT_WITH_FLUSH = true

    MODE_STANDALONE = 'Standalone'
    MODE_SOLRCLOUD = 'SolrCloud'

    config_set_default :include_tag_key, false
    config_set_default :include_time_key, false

    config_param :base_url, :string, :default => nil,
                 :desc => 'The Solr base url (for example http://localhost:8983/solr).'

    config_param :zk_host, :string, :default => nil,
                 :desc => 'The ZooKeeper connection string that SolrCloud refers to (for example localhost:2181/solr).'

    config_param :collection, :string, :default => DEFAULT_COLLECTION,
                 :desc => 'The Solr collection/core name (default collection1).'

    config_param :ignore_undefined_fields, :bool, :default => DEFAULT_IGNORE_UNDEFINED_FIELDS,
                 :desc => 'Ignore undefined fields in the Solr schema.xml.'

    config_param :tag_field, :string, :default => DEFAULT_TAG_FIELD,
                 :desc => 'A field name of fluentd tag in the Solr schema.xml (default time).'

    config_param :time_field, :string, :default => DEFAULT_TIME_FIELD,
                 :desc => 'A field name of event timestamp in the Solr schema.xml (default time).'
    config_param :time_format, :string, :default => DEFAULT_TIME_FORMAT,
                 :desc => 'The format of the time field (default %d/%b/%Y:%H:%M:%S %z).'
    config_param :millisecond, :bool, :default => DEFAULT_MILLISECOND,
                 :desc => 'Output millisecond to Solr (default false).'

    config_param :flush_size, :integer, :default => DEFAULT_FLUSH_SIZE,
                 :desc => 'A number of events to queue up before writing to Solr (default 100).'

    config_param :commit_with_flush, :bool, :default => DEFAULT_COMMIT_WITH_FLUSH,
                 :desc => 'Send commit command to Solr with flush (default true).'

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      config_set_default :chunk_keys, ['tag']
    end

    def initialize
      super
    end

    def configure(conf)
      compat_parameters_convert(conf, :inject)
      super
    end

    def start
      super

      @mode = nil
      if ! @base_url.nil? then
        @mode = MODE_STANDALONE
      elsif ! @zk_host.nil?
        @mode = MODE_SOLRCLOUD
      end

      @solr = nil
      @zk = nil

      if @mode == MODE_STANDALONE then
        @solr = RSolr.connect :url => @base_url.end_with?('/') ? @base_url + @collection : @base_url + '/' + @collection
      elsif @mode == MODE_SOLRCLOUD then
        @zk = ZK.new(@zk_host)
        cloud_connection = RSolr::Cloud::Connection.new(@zk)
        @solr = RSolr::Client.new(cloud_connection, read_timeout: 60, open_timeout: 60)
      end

      # Get unique key field from Solr
      @unique_key = get_unique_key

      # Get fields from Solr
      @fields = get_fields
    end

    def shutdown
      super

      unless @zk.nil? then
        @zk.close
      end
    end

    def format(tag, time, record)
      [time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      documents = []

      # Get fluentd tag
      tag = chunk.metadata.tag

      chunk.msgpack_each do |time, record|
        record = inject_values_to_record(tag, time, record)

        # Set unique key and value
        unless record.has_key?(@unique_key) then
          record.merge!({@unique_key => SecureRandom.uuid})
        end

        # Set Fluentd tag to Solr tag field
        unless record.has_key?(@tag_field) then
          record.merge!({@tag_field => tag})
        end

        # Set time
        tmp_time = Time.at(time).utc
        if record.has_key?(@time_field) then
          # Parsing the time field in the record by the specified format.
          begin
            tmp_time = Time.strptime(record[@time_field], @time_format).utc
          rescue Exception => e
            log.warn "An error occurred in parsing the time field: #{e.message}"
          end
        end
        if @millisecond then
          record.merge!({@time_field => '%s.%03dZ' % [tmp_time.strftime('%FT%T'), tmp_time.usec / 1000.0]})
        else
          record.merge!({@time_field => tmp_time.strftime('%FT%TZ')})
        end

        # Ignore undefined fields
        if @ignore_undefined_fields then
          record.each_key do |key|
            unless @fields.include?(key) then
              record.delete(key)
            end
          end
        end

        # Add record to documents
        documents << record

        # Update when flash size is reached
        if documents.count >= @flush_size
          update documents
          documents.clear
        end
      end

      # Update remaining documents
      update documents unless documents.empty?
    end

    def update(documents)
      begin
        if @mode == MODE_STANDALONE then
          @solr.add documents, :params => {:commit => @commit_with_flush}
        elsif @mode == MODE_SOLRCLOUD then
          @solr.add documents, collection: @collection, :params => {:commit => @commit_with_flush}
        end
        log.debug "Sent #{documents.count} document(s) to Solr"
      rescue Exception
        log.warn "An error occurred while sending #{documents.count} document(s) to Solr"
      end
    end

    def get_unique_key
      unique_key = 'id'

      begin
        response = nil
        if @mode == MODE_STANDALONE then
          response = @solr.get 'schema/uniquekey'
        elsif @mode == MODE_SOLRCLOUD then
          response = @solr.get 'schema/uniquekey', collection: @collection
        end
        unique_key = response['uniqueKey']
        log.debug "Unique key: #{unique_key}"
      rescue Exception
        log.warn 'An error occurred while getting unique key'
      end

      return unique_key
    end

    def get_fields
      fields = []

      begin
        response = nil

        if @mode == MODE_STANDALONE then
          response = @solr.get 'schema/fields'
        elsif @mode == MODE_SOLRCLOUD then
          response = @solr.get 'schema/fields', collection: @collection
        end
        response['fields'].each do |field|
          fields.push(field['name'])
        end
        log.debug "Fields: #{fields}"
      rescue Exception
        log.warn 'An error occurred while getting fields'
      end

      return fields
    end
  end
end
