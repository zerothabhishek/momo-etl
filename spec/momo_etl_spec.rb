

class SampleEtl < MomoEtl::Job

  Rows = [
    { a: 10, b: 20 },
    { a: 99, b: 88 }
  ]

  def read
    Rows.each do |row|
      yield row
    end
  end

  def transform_1(row)
    row[:c] = 3
    row
  end

  def transform_2(row)
    row
  end

  def write(row)
  end
end


RSpec.describe MomoEtl::Job do

  describe "#run" do

    let(:etl_job) { SampleEtl.new(fail_fast: true) }

    let(:first_row) { SampleEtl::Rows[0] }
    let(:second_row) { SampleEtl::Rows[1] }

    it "executes the transform_1" do
      expect(etl_job).to receive(:transform_1).with(first_row).once
      expect(etl_job).to receive(:transform_1).with(second_row).once

      etl_job.run
    end

  end
end