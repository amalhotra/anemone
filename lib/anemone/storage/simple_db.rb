require 'aws-sdk'
require 'digest'
require 'json'

module Anemone
  module Storage
    class PageRecord < AWS::Record::Base
      
      set_domain_name 'pages'
      
      S3_FIELDS = %w(body headers data)
      DUP_FIELDS = %w(url)
      
      string_attr :url
      string_attr :redirect_to
      boolean_attr :visited
      integer_attr :depth
      string_attr :referer
      integer_attr :response_time
      integer_attr :code
      boolean_attr :fetched
      string_attr :links, :set => true
      string_attr :s3
      
      def update_rec(url,h,bucket)
        h[:url] = url
        h[:s3] = update_s3_fields(h,bucket)
        sdb_h = h.reject{|k,v| S3_FIELDS.include?(k) }
        self.update_attributes!(sdb_h)
      end
      
      def update_s3_fields(p_hash,bucket)
        h = {}
        o = bucket.objects[digest(p_hash[:url])]
        S3_FIELDS+DUP_FIELDS.each do |k|
          h[k] = p_hash[k]
        end
        o.write(h.to_json,{:content_type => 'application/json'})
        o.key
      end
            
      def self.rec(url)
        PageRecord.first(:where => {:url => url.strip})
      end
      
      def digest(data)
        Digest::MD5.hexdigest(data.strip)
      end
      
    end
    
    class SimpleDb
      
      def initialize(db,domain,bucket,purge=false)
        @db = db
        @domain = domain
        @bucket = bucket
        @domain.delete! if purge && @domain.exists?
        self
      end
      
      def [](url)
        page_rec = PageRecord.rec(url.to_s)
        load_page(page_rec) if page_rec
      end
      
      def []=(url,page)
        h = page.to_hash
        page_rec = PageRecord.rec(url.to_s)
        page_rec = PageRecord.new unless page_rec
        page_rec.update_rec(url,h,@bucket)
      end
      
      def delete(url)
        page_rec = PageRecord.rec(url.to_s)
        s3 = @bucket.objects[page_rec.s3]
        s3.delete && page_rec.delete
      end
      
      def each
        PageRecord.all.each do |pr|
          page = load_page(pr)
          yield page.url.to_s,page
        end
      end
      
      def merge!(hash)
        hash.each{|k,v| self[k] = v}
        self
      end
      
      def close
      end
      
      def size
        PageRecord.count
      end
      
      def keys
        keys = []
        self.each{ |k,v| keys << k.to_s }
        keys
      end
      
      def has_key?(url)
        !!PageRecord.rec(url.to_s)
      end
      
      private
      
      def load_page(page_rec)
        s3_json = @bucket.objects[page_rec.s3]
        h = page_rec.attributes
        h.merge!(JSON.parse(s3_json))
        Page.from_hash(h)
      end
            
      
    end
  end
end