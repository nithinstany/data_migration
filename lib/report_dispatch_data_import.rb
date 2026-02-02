class ReportDispatchRelatedDataImport

  def self.import(venue_id)
    report_dispatch_count = BackupReader.count('report_dispatches', { venue_id: venue_id })

    last_report_dispatch_id = 0

    batch_size = 10
    loops = (report_dispatche_count.to_f / batch_size).ceil

    (0..loops).each do |loop|
      puts "Processing loop #{loop + 1} of #{loops}"
      report_dispatches = BackupReader.fetch('report_dispatches', { venue_id: venue_id }, { id: last_report_dispatch_id }, batch_size)

      if report_dispatches.blank?
        break
      end

      report_dispatches.each do |report_dispatch|
        MainDbWriter.upsert('report_dispatches', report_dispatch)

        last_report_dispatch_id = report_dispatch['id']

        ['report_print_details', 'outbound_logs', 'report_dispatch_extra_fields'].each do |table|
          BackupReader.fetch(table, { report_dispatch_id: report_dispatch['id'] }).each do |record|
            MainDbWriter.upsert(table, record)
          end
        end
      end
    end
  end
end