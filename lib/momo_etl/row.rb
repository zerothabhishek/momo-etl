module MomoEtl
  module Row
    def meta
      @meta ||= {}
    end

    def skip!
      @skip = true
    end

    def skip?
      @skip
    end

    def applicable?(step)
      return false if @skip
      true
    end
  end
end