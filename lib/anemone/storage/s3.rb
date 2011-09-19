require 'aws-sdk'
require 'digest'
require 'json'

module Anemone
  module Storage
    class S3
      
      def initialize(bucket,purge=false)
        @bucket = bucket
        @bucket.versions.each{|version| version.delete } if purge
        self
      end
      
      def [](url)
        o = @bucket.objects[digest(url)]
        if o.exists?
          json = o.read
          load_page(json)
        end
      end
      
      def []=(url,page)
        h = page.to_hash
        o = @bucket.objects[digest(url)]
        json = h.to_json
        o.write(json,{:content_type => 'application/json'})
      end
      
      def delete(url)
        page = self[url]
        o = @bucket.objects[digest(url)]
        o.delete
        page
      end
      
      def each
        @bucket.objects.each do |o|
          page = load_page(o.read)
          yield page.url.to_s,page
        end
      end
      
      def merge!(hash)
        hash.each{|k,v| self[k] = v }
        self
      end
      
      def close
      end
      
      def size
        @bucket.objects.size
      end
      
      def keys
        @bucket.objects.map(&:key)
      end
      
      def has_key?(url)
        @bucket.objects[digest(url)].exists?
      end
      
      protected
      
      def digest(data)
        Digest::MD5.hexdigest(data.strip)
      end
      
      def load_page(json)
        Page.from_hash(JSON.parse(json))
      end
      
    end
  end
end
