require 'test_helper'

class MomoEtlReadTest < Minitest::Test

  ## Samples ####################################

  module SampleEtl

    attr_reader :read_list

    def initialize
      @read_list = []
    end

    def read
      args[:rowset].lazy.map do |r|
        @read_list << r
        r
      end
    end
  end

  module SampleEtl2

    def read
      [{ a: 2 }]
    end
  end

  ## Tests ####################################

  def setup
    @sample_rows = [{ a: 10, b: 20 }, { a: 100, b: 200 }]
  end

  # Extraction happens in `read` method
  def test_read

    etl = Class.new(MomoEtl::Job){ include SampleEtl }.new
    etl.run(rowset: @sample_rows)

    assert_equal @sample_rows, etl.read_list
  end

  # Fails if read method is not defined
  def test_read1

    klass = Class.new(MomoEtl::Job)
    assert_raises("ETL must have a `read` method") do
      klass.new.run(rowset: @sample_rows)
    end
  end

  # `read` method should yield a hash to the supplied block
  #def test_read2
  #
  #  klass = Class.new(MomoEtl::Job){ include SampleEtl2 }
  #  j = klass.new
  #
  #  t = nil
  #  j.read { |h| t = h }
  #  assert_equal Hash, t.class
  #end

  # `read` method should give an enumerator
  def test_read2

    etl = Class.new(MomoEtl::Job){ include SampleEtl2 }.new

    x = etl.read
    assert x.is_a?(Enumerable)
  end
end
