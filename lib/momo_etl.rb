require "momo_etl/version"

#
# read, transforms, write
# record errors during transforms and writes
# validate header and stop if wrong
#

module MomoEtl
  class Job

    attr_reader :momo, :args, :errors
    MomoData = Struct.new(:args, :errors)

    def run(args = {})

      @args    = args
      @errors  = []
      @outputs = []

      @momo ||= MomoData.new(@args, @errors)

      i = 0

      checks!

      read do |row|

        if i==0
          validate(row.keys)
          before_all
        end

        result = process_row(row)
        @outputs << result

        i += 1
      end

      after_all
    end

    def checks!
      ## How to read. Must take a block, and yield a row-hash to it
      unless self.respond_to?(:read)
        raise "ETL must have a `read` method"
      end

      ## How to write the row.
      #unless self.respond_to?(:write)
      #  raise "ETL must have a `write` method"
      #end
    end

    # Called before reading the rows
    # Reading will stop if this returns false
    def valid?(header = nil)
    end

    def before_row(row)
    end

    def after_row(row)
    end

    def before_all
    end

    def after_all
    end

    private

      def valid?(header)
        true
      end

      def validate(header)
        unless valid?(header)
          raise InvalidHeader
        end
      end

      def process_row(row)
        row = row.extend(Row) unless row.respond_to?(:meta)

        before_row(row)

        row = run_transforms(row)
        if row.applicable?('write')
          with_ceremony do
            write(row)
          end
        end

        row = after_row(row)
        row
      end

      def run_transforms(row)
        transforms.each do |transform|
          
          # Skip if at any point it is asked to skip
          unless row.applicable?("transform_#{transform}")
            return row
          end

          row = run_single_transform(row, transform)
        end
        row
      end

      def run_single_transform(row, transform)
        
        # Keep a pointer to the original meta-data
        orig_meta = row.meta

        result = self.public_send(transform, row)

        # TODO: raise exception if result is not a hash

        # For the next tranformation,
        # - the result is passed as the row
        # - extend the row if it doesn't respond to :meta
        # - merge the original meta with the row meta
        row = result
        row = row.extend(Row) unless result.respond_to?(:meta)
        row.meta.merge!(orig_meta)

        row
      end

      def with_ceremony
        begin
          yield
        rescue => e
          @errors << e
          nil
        end
      end

      def transforms
        self.public_methods.grep(/^transform_*/)
      end
  end

  class InvalidHeader < Exception
  end

  module Row
    def meta
      @meta ||= {}
    end

    def skip!
      @skip = true
    end

    def skip?
      @skip
    end

    def applicable?(step)
      return false if @skip
      true
    end
  end
end