module BackupReader
  def self.fetch(table, where = {})
    conditions = where.keys.map { |k| "#{k} = ?" }.join(" AND ")
    sql = "SELECT * FROM #{table}"
    sql += " WHERE #{conditions}" if where.any?

    BackupDbBase.connection.exec_query(
      ActiveRecord::Base.send(:sanitize_sql_array, [sql, *where.values])
    ).to_a
  end
end