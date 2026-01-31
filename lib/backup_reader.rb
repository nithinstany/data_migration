module BackupReader
  def self.fetch(table, where = {}, greater_than = {}, limit = nil)
    conditions = where.keys.map { |k| "#{k} = ?" }

    if greater_than.any?
      conditions << greater_than.keys.map { |k| "#{k} > ?" }
    end

    final_conditions = conditions.join(" AND ")

    sql = "SELECT * FROM #{table}"
    sql += " WHERE #{final_conditions}"
    sql += " ORDER BY id ASC"
    sql += " LIMIT #{limit}" if limit.present?

    BackupDbBase.connection.exec_query(
      ActiveRecord::Base.send(:sanitize_sql_array, [sql, *where.values, *greater_than.values])
    ).to_a
  end

  def self.count(table, where = {})
    conditions = where.keys.map { |k| "#{k} = ?" }.join(" AND ")
    sql = "SELECT COUNT(*) FROM #{table}"
    sql += " WHERE #{conditions}" if where.any?

    BackupDbBase.connection.exec_query(
      ActiveRecord::Base.send(:sanitize_sql_array, [sql, *where.values])
    ).to_a[0]['COUNT(*)']
  end
end