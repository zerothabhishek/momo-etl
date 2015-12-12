require 'test_helper'

class WriteTest < MiniTest::Test

  ## Samples ####################################

  module SampleEtl
    def read
      args[:rowset].each{ |r| yield r }
    end

    def write(row)
      args[:db] << row
    end
  end

  module SampleEtl1
    def read
      args[:rowset].each{ |r| yield r }
    end
  end

  ## Tests ####################################

  def setup
    @fake_db = []
    @sample_rows = [{ a: 10, b: 20 }, { a: 100, b: 200 }]
  end

  # Load happens in `write` method
  def test__write

    klass = Class.new(MomoEtl::Job){ include SampleEtl }
    klass.new(rowset: @sample_rows, db: @fake_db).run

    assert_equal @fake_db, @sample_rows
  end

  # Fails if `write` method is not defined
  def test__write1

    klass = Class.new(MomoEtl::Job){ include SampleEtl1 }

    assert_raises("ETL must have a `write` method") do
      klass.new(rowset: @sample_rows).run
    end
  end
end
