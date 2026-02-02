class PrescriptionRelatedDataImport
  
  def self.import(venue_id)
    prescription_count = BackupReader.count('prescriptions', { venue_id: venue_id })

    last_prescription_id = 0

    batch_size = 100
    loops = (prescription_count.to_f / batch_size).ceil

    (0..loops).each do |loop|
      puts "Processing loop #{loop + 1} of #{loops}"
      prescriptions = BackupReader.fetch('prescriptions', { venue_id: venue_id }, { id: last_prescription_id }, batch_size)

      if prescriptions.blank?
        break
      end

      prescriptions.each do |prescription|
        MainDbWriter.upsert('prescriptions', prescription)

        last_prescription_id = prescription['id']

        ['prescribed_tests', 'prescription_samples', 'prescription_packages', 'prescription_bill_item_masters', 'prescription_images', 'miscellaneous_images', 'prescription_sample_precribed_tests', 'outbound_logs', 'prescription_extra_fields', 'turn_around_times'].each do |prescription_has_table|
          BackupReader.fetch(prescription_has_table, { prescription_id: prescription['id'] }).each do |record|
            MainDbWriter.upsert(prescription_has_table, record)

            if prescription_has_table == "prescribed_tests"
              ['reports', 'outsource_uploads', 'outsource_lab_payments', 'prescribed_test_pdf_reports', 'prescribed_test_notes', 'treated_by_prescribed_tests'].each do |prescribed_test_has_table|
                BackupReader.fetch(prescribed_test_has_table, { prescribed_test_id: record['id'] }).each do |prescribed_has_record|
                  
                  MainDbWriter.upsert(prescribed_test_has_table, prescribed_has_record)
 
                  if prescribed_test_has_table == 'reports'
                    puts "prescriptions ~> prescribed_tests ~> reports ~> #{prescribed_has_record['id']}"
                    
                    ['report_values', 'report_versions', 'report_value_versions', 'report_interfaces', 'report_extra_fields'].each do |report_has_table|
                      puts "prescriptions ~> prescribed_tests ~> reports ~> #{prescribed_has_record['id']}" 
                     
                      BackupReader.fetch(report_has_table, { report_id: prescribed_has_record['id'] }).each do |report_has_record|
                        MainDbWriter.upsert(report_has_table, report_has_record)

                        if report_has_table == 'report_values'
                          ['report_table_format_results', 'report_multi_parameter_results', 'report_group_rows', 'test_image_results'].each do |rep_value_table|
                            BackupReader.fetch(rep_value_table, { report_value_id: report_has_record['id'] }).each do |rep_value_has_record|
                              MainDbWriter.upsert(rep_value_table, rep_value_has_record)
                            end
                          end
                        end
                      end
                    end
                  end
                end
              end
            end
          end
        end
      end
    end
  end
end