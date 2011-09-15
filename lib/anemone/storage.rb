module Anemone
  module Storage

    def self.Hash(*args)
      hash = Hash.new(*args)
      # add close method for compatibility with Storage::Base
      class << hash; def close; end; end
      hash
    end

    def self.PStore(*args)
      require 'anemone/storage/pstore'
      self::PStore.new(*args)
    end

    def self.TokyoCabinet(file = 'anemone.tch')
      require 'anemone/storage/tokyo_cabinet'
      self::TokyoCabinet.new(file)
    end

    def self.MongoDB(mongo_db = nil, collection_name = 'pages')
      require 'anemone/storage/mongodb'
      mongo_db ||= Mongo::Connection.new.db('anemone')
      raise "First argument must be an instance of Mongo::DB" unless mongo_db.is_a?(Mongo::DB)
      self::MongoDB.new(mongo_db, collection_name)
    end

    def self.Redis(opts = {})
      require 'anemone/storage/redis'
      self::Redis.new(opts)
    end
    
    # S3 as storage
    # purge would delete all objects in bucket by default
    def self.S3(s3 = nil, bucket = nil,purge = true)
      require 'anemone/storage/s3'
      s3 ||= AWS::S3.new
      raise "First argument must be an instance of AWS::S3" unless s3.is_a?(AWS::S3)
      bucket ||= s3.buckets.create('pages')
      raise "Second argument must be an instance of AWS::S3::Bucket" unless bucket.is_a?(AWS::S3::Bucket)
      self::S3.new(s3,bucket,purge)
    end

  end
end
