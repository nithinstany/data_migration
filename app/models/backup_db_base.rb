class BackupDbBase < ActiveRecord::Base
  self.abstract_class = true
  establish_connection :backup_db
end