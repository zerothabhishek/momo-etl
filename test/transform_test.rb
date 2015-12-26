require 'test_helper'

class TransformTest < MiniTest::Test

  ## Samples ####################################

  module SampleEtl

    attr_reader :es # executions list
    def initialize
      @es = []
    end

    def read
      args[:rowset]
    end

    def transform_1(row)
      row[:c] = 30
      @es << :transform_1
      row
    end

    def transform_2(row)
      @es << :transform_2
      row
    end
  end

  module SampleEtl1

    attr_reader :seq # executions sequence
    def initialize
      @seq = []
    end

    def read
      args[:rowset]
    end

    def transform_a(row)
      @seq << :transform_a
      row
    end

    def transform_b(row)
      @seq << :transform_b
      row
    end

    def transform_c(row)
      @seq << :transform_c
      row
    end
  end

  module SampleEtl2

    attr_reader :inputs_for, :outputs_for

    def initialize
      @inputs_for = {}
      @outputs_for = {}
    end

    def read
      args[:rowset]
    end

    def transform_a(row)
      inputs(:a, row)
      row[:a] = 1000
      outputs(:a, row)
      row
    end

    def transform_b(row)
      inputs(:b, row)
      row[:b] = 2000
      outputs(:b, row)
      row
    end

    def transform_c(row)
      inputs(:c, row)
      row[:c] = 1000
      outputs(:c, row)
      row
    end

    def inputs(transform, row)
      @inputs_for[transform] ||= []
      @inputs_for[transform] << row
    end

    def outputs(transform, row)
      @outputs_for[transform] ||= []
      @outputs_for[transform] << row
    end

  end


  ## Tests ####################################

  def setup
    @sample_rows = [{ a: 10, b: 20 }]
  end

  def test_transforms0 # Transform methods get executed

    etl = Class.new(MomoEtl::Base){ include SampleEtl }.new
    etl.run(rowset: @sample_rows)

    executions = etl.es
    assert executions.include?(:transform_1) &&
           executions.include?(:transform_2)
  end

  # Transform methods are run in order of appearance
  def test_transforms1

    etl = Class.new(MomoEtl::Base){ include SampleEtl1 }.new
    etl.run(rowset: @sample_rows)

    sequence = etl.seq
    assert_equal [:transform_a, :transform_b, :transform_c], sequence
  end

  # Input to each Transform method is the output of the previous one
  def test_transforms2

    etl = Class.new(MomoEtl::Base){ include SampleEtl2 }.new
    etl.run(rowset: @sample_rows)

    assert etl.outputs_for[:a] == etl.inputs_for[:b] &&
           etl.outputs_for[:b] == etl.inputs_for[:c]
  end

  # TODO: Any failures in Transforms methods are recorded as `errors`
  #def test__transforms3
  #end
end
