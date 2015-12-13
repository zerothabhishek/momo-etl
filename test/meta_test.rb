require 'test_helper'

class MetaTest < MiniTest::Test

  ## Samples ####################################

  module SampleEtl
    def read
      args[:rowset].each{ |r| yield r }
    end

    def transform_a(row) ## adds :id to meta
      row.meta[:id] = row.hash
      args[:metas] << row.meta
      row
    end

    def write(row); end
  end

  module SampleEtl2
    def read
      args[:rowset].each{ |r| yield r }
    end

    def transform_a(row) ## adds :id to meta
      row.meta[:id] = row.hash
      row
    end

    def transform_b(row) ## adds :sum to meta
      row.meta[:sum] = row.values.reduce(&:+)
      row
    end

    def transform_c(row)
      args[:metas] << row.meta
      row
    end

    def write(row); end
  end

  ## Tests ####################################

  def setup
    @fake_db = []
    @sample_rows = [ { a: 10, b: 20 }, { a: 100, b: 200 } ]
  end

  # Each row gets a meta attribute hash
  def test_meta1
    metas = []
    klass = Class.new(MomoEtl::Job){ include SampleEtl }
    klass.new.run(rowset: @sample_rows, metas: metas)

    assert metas.all? do |meta|
      Hash === meta &&
      meta.has_key?(:id)
    end
  end

  # :meta data gets merged after transforms
  def test_meta2
    metas = []
    klass = Class.new(MomoEtl::Job){ include SampleEtl2 }
    klass.new.run(rowset: @sample_rows, metas: metas)

    assert metas.all? do |meta0|
      meta0.has_key?(:id) &&
      meta0.has_key?(:sum)
    end
  end
end
