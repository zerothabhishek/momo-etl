require 'test_helper'

class WriteTest < MiniTest::Test

  ## Samples ####################################

  module SampleEtl

    attr_reader :write_list

    def initialize
      @write_list = []
    end

    def read
      args[:rowset]
    end

    def write(row)
      @write_list << row
    end
  end

  module SampleEtl2

    attr_reader :output_b, :write_input

    def initialize
      @output_b = []
      @write_input = []
    end

    def read
      args[:rowset]
    end

    def transform_a(row)
      row[:x] = 500
      row
    end

    def transform_b(row)
      row[:y] = 600
      @output_b << row
      row
    end

    def write(row)
      @write_input << row
    end
  end

  ## Tests ####################################

  def setup
    @sample_rows = [{ a: 10, b: 20 }, { a: 100, b: 200 }]
  end

  # Load happens in `write` method
  # Given row is written back when there are no transforms
  def test_write

    etl = Class.new(MomoEtl::Job){ include SampleEtl }.new
    etl.run(rowset: @sample_rows)

    assert_equal @sample_rows, etl.write_list
  end

  # output from the last transform is the input to write
  def test_write2

    etl = Class.new(MomoEtl::Job){ include SampleEtl2 }.new
    etl.run(rowset: @sample_rows)

    assert etl.output_b == etl.write_input
  end
end
