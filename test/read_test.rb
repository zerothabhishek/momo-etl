require 'test_helper'

class MomoEtlReadTest < Minitest::Test

  ## Samples ####################################

  module SampleEtl

    def read
      args[:rowset].each{ |r| yield r }
    end

    # Without any transforms, the write should get all the data that was read
    def write(row)
      args[:db] << row
    end
  end

  module SampleEtl2

    def read
      yield({ a: 2 })
    end

    def write(row); end
  end

  ## Tests ####################################

  def setup
    @fake_db = []
    @sample_rows = [{ a: 10, b: 20 }, { a: 100, b: 200 }]
  end

  # Extraction happens in `read` method
  def test_read

    klass = Class.new(MomoEtl::Job){ include SampleEtl }
    klass.new.run(rowset: @sample_rows, db: @fake_db)

    assert_equal @sample_rows, @fake_db
  end

  # Fails if read method is not defined
  def test_read1

    klass = Class.new(MomoEtl::Job)
    assert_raises("ETL must have a `read` method") do
      klass.new.run(rowset: @sample_rows)
    end
  end

  # `read` method should yield a hash to the supplied block
  def test_read2

    klass = Class.new(MomoEtl::Job){ include SampleEtl2 }
    j = klass.new

    t = nil
    j.read { |h| t = h }
    assert_equal Hash, t.class
  end
end
