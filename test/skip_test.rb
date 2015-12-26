require 'test_helper'

class SkipTest < MiniTest::Test

  ## Samples ####################################

  module SampleEtl

    attr_reader :el # executions list
    def initialize
      @el = []
    end

    def read
      args[:rowset]
    end

    def before_row(row)
      row.skip!
    end

    def transform_1(row)
      row[:c] = 100
      @el << :transform_1
      row
    end

    def transform_2(row)
      row[:d] = 200
      @el << :transform_2
      row
    end
  end

  module SampleEtl2

    attr_reader :el # executions list
    def initialize
      @e  = []
      @el = []
    end

    def read
      args[:rowset]
    end

    def before_row(row)
      @e = []

      row.skip! if row[:id] == 1
    end

    def transform_1(row)
      row[:c] = 100
      @e << :transform_1
      row
    end

    def transform_2(row)
      row[:d] = 200
      @e << :transform_2
      row
    end

    def after_row(row)
      @el << @e
    end
  end

  module SampleEtl3

    attr_reader :el # executions list
    def initialize
      @el = []
    end

    def read
      args[:rowset]
    end

    def transform_1(row)
      row.skip!
      @el << :transform_1
      row
    end

    def transform_2(row)
      row[:d] = 200
      @el << :transform_2
      row
    end
  end


  ## Tests ####################################

  def setup
    @sample_rows = [{ id: 1, a: 10, b: 20 }, { id: 2, a: 100, b: 200 }]
  end

  # Transforms after skip! are not executed for a row
  def test_skip

    etl = Class.new(MomoEtl::Base){ include SampleEtl }.new
    etl.run(rowset: @sample_rows)

    execution_list = etl.el
    assert !execution_list.include?(:transform_1) &&
           !execution_list.include?(:transform_2)
  end

  # Transforms are skipped only for the current row
  def test_skip2

    etl = Class.new(MomoEtl::Base){ include SampleEtl2 }.new
    etl.run(rowset: @sample_rows)

    list1 = etl.el[0]
    list2 = etl.el[1]

    row1_skipped     = !list1.include?(:transform_1) && !list1.include?(:transform_2)
    row2_not_skipped =  list2.include?(:transform_1) &&  list2.include?(:transform_2)

    assert row1_skipped && row2_not_skipped
  end

  # The current transform is not skipped
  def test_skip3

    etl = Class.new(MomoEtl::Base){ include SampleEtl3 }.new
    etl.run(rowset: [@sample_rows[0]])

    execution_list = etl.el
    assert execution_list.include?(:transform_1)
  end
end
