require 'test_helper'

class TransformTest < MiniTest::Test

  ## Samples ####################################

  module SampleEtl
    def read
      yield args[:rowset][0]
    end

    def transform_1(row)
      row[:c] = 30
      row
    end

    def write(row)
      args[:db] << row
    end
  end

  module SampleEtl1
    def read
      args[:rowset].each{ |r| yield r }
    end

    def transform_a(row)
      args[:seq] << "transform_a"
      row
    end

    def transform_b(row)
      args[:seq] << "transform_b"
      row
    end

    def transform_c(row)
      args[:seq] << "transform_c"
      row
    end

    def write(row)
    end
  end

  module SampleEtl2
    def read
      args[:rowset].each{ |r| yield r }
    end

    def transform_a(row)
      args[:i][:transform_a] = row
      row[:a] = 1000
      args[:o][:transform_a] = row
      row
    end

    def transform_b(row)
      args[:i][:transform_b] = row
      row[:b] = 2000
      args[:o][:transform_b] = row
      row
    end

    def transform_c(row)
      args[:i][:transform_c] = row
      row[:c] = 1000
      args[:o][:transform_c] = row
      row
    end

    def write(row)
    end
  end


  ## Tests ####################################

  def test__transforms0 # Transform methods get executed

    klass = Class.new(MomoEtl::Job){ include SampleEtl }

    fake_db = []
    row   = { a: 10, b: 20 }
    row_t = { a: 10, b:20, c:30 }

    j = klass.new(rowset: [row], db: fake_db); j.run
    assert_equal(row_t, fake_db[0])
  end

  # Transform methods are run in order of appearance
  def test__transforms1
    row = { a: 10, b: 20 }
    seq = []
    klass = Class.new(MomoEtl::Job){ include SampleEtl1 }
    klass.new(rowset: [row], seq: seq).run

    assert_equal %w(transform_a transform_b transform_c), seq
  end

  # Input to each Transform method is the output of the previous one
  def test__transforms2
    row = { a: 10, b: 20 }
    inputs = {}
    outputs = {}

    klass = Class.new(MomoEtl::Job){ include SampleEtl2 }
    klass.new(rowset: [row], i: inputs, o: outputs).run

    assert outputs[:transform_a] == inputs[:transform_b] &&
           outputs[:transform_b] == inputs[:transform_c]
  end

  # TODO: Any failures in Transforms methods are recorded as `errors`
  #def test__transforms3
  #end
end
