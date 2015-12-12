require "momo_etl/version"

#
# read, transforms, write
# record errors during transforms and writes
# validate header and stop if wrong
#

module MomoEtl
  class Job

    attr_reader :args, :errors, :row_data, :fail_fast

    def initialize(args = {})
      @args = args
      @errors = []
      @row_data = {}
      @fail_fast = args[:fail_fast] || false
    end

    def run
      i = 0

      checks!

      read do |row|

        row = row.extend(Row)

        if i==0
          validate(row.keys)
          before_all
        end

        before_row(row)

        row = run_transforms(row)
        with_ceremony do
          write(row)
        end

        after_row(row)

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
      unless self.respond_to?(:write)
        raise "ETL must have a `write` method"
      end
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

      def transforms
        self.public_methods.grep(/^transform_*/)
      end

      def run_transforms(row)
        transforms.each do |transform|
          
          # TODO: remove
          # Skip if the transform is not defined, by any chance
          next unless self.respond_to?(transform)

          row = run_single_transform(row, transform)
        end
        row
      end

      def run_single_transform(row, transform)
        
        # Keep a pointer to the original data
        orig_data = row.data

        # invoke the transform, and take note of errors
        row1 = with_ceremony do
          self.public_send(transform, row)
        end

        # TODO: raise exception if row1 is not a hash
        # If the transform returns nil, use the older row object
        # If the transform returns non-nil, assign it to the row variable
        if row1.nil?
          row = row
        else
          row = row1
        end

        # extend the row again, if its a new one, and doesn't respond to :data
        row = row.extend(Row)   unless row.respond_to?(:data)

        # merge the original data with the row data
        row.data.merge!(orig_data)

        row
      end

      def with_ceremony
        begin
          yield
        rescue => e
          @errors << e
          if fail_fast
            raise
          end
          nil
        end
      end

  end

  class InvalidHeader < Exception
  end

  module Row
    def data
      @data ||= {}
    end
  end

end