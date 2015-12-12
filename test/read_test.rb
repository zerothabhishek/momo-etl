require 'test_helper'

class MomoEtlReadTest < Minitest::Test

  ## Samples ####################################

  module SampleEtl

    def read
      args[:rowset].each{ |r| yield r }
    end

    # Without any transforms, the write should get all the data that was read
    def write(row);
      args[:db] << row
    end
  end

  ## Tests ####################################

  def setup
    @fake_db = []
    @sample_rows = [{ a: 10, b: 20 }, { a: 100, b: 200 }]
  end

  # Extraction happens in `read` method
  def test__read

    klass = Class.new(MomoEtl::Job){ include SampleEtl }
    klass.new(rowset: @sample_rows, db: @fake_db).run

    assert_equal @sample_rows, @fake_db
  end

  # Fails if read method is not defined
  def test__read1

    klass = Class.new(MomoEtl::Job)
    assert_raises("ETL must have a `read` method") do
      klass.new(rowset: @sample_rows).run
    end
  end
end
