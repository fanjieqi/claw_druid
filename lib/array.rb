class Array
  def except
    result = self.select{|value| yield(value) }
    self.delete_if{|value| yield(value) }
    result
  end
end
