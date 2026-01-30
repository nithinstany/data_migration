module MainDbWriter
  def self.upsert(table, attrs)
      record = MainDbBase.connection.exec_query(
        "SELECT * FROM #{table} WHERE id = #{attrs['id']}"
      ).to_a


      

      if record.blank?

        final_attrs = attrs.transform_values! do |v|
          case v
          when Date
            v.strftime('%Y-%m-%d')
          when Time, DateTime, ActiveSupport::TimeWithZone
            v.strftime('%Y-%m-%d %H:%M:%S')
          else
            v
          end
        end

        puts final_attrs.inspect

        columns = final_attrs.keys.join(', ')
        values = final_attrs.values.map { |v| MainDbBase.connection.quote(v) }.join(', ')
        MainDbBase.connection.execute(
          "INSERT INTO #{table} (#{columns}) VALUES (#{values})"
        )
      end 
  end
end