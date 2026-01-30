class MainDbBase < ActiveRecord::Base
  self.abstract_class = true
  establish_connection :primary
end