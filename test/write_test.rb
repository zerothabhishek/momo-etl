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

  module SampleEtl2
    def read
      args[:rowset].each{ |r| yield r }
    end

    def transform_a(row)
      row[:x] = 500
      row
    end

    def transform_b(row)
      row[:y] = 600
      args[:o][:transform_b] = row
      row
    end

    def write(row)
      args[:i][:write] = row
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
    klass.new.run(rowset: @sample_rows, db: @fake_db)

    assert_equal @fake_db, @sample_rows
  end

  # Fails if `write` method is not defined
  def test__write1

    klass = Class.new(MomoEtl::Job){ include SampleEtl1 }

    assert_raises("ETL must have a `write` method") do
      klass.new.run(rowset: @sample_rows)
    end
  end

  # output from the last transform is the input to write
  def test_write2
    inputs = {}
    outputs = {}
    klass = Class.new(MomoEtl::Job){ include SampleEtl2 }
    klass.new.run(rowset: @sample_rows, i: inputs, o: outputs)

    assert outputs[:transform_b] == inputs[:write]
  end
end
