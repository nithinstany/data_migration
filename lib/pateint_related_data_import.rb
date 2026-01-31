class PateintRelatedDataImport
  def self.import

    patient_count = BackupReader.count('patients', { venue_id: 974638029 })

    last_patient_id = 0

    batch_size = 10
    loops = (patient_count.to_f / batch_size).ceil

    (0..loops).each do |loop|
      puts "Processing loop #{loop + 1} of #{loops}"
      patients = BackupReader.fetch('patients', { venue_id: 974638029 }, { id: last_patient_id }, batch_size)

      if patients.blank?
        break
      end

      patients.each do |patient|
        MainDbWriter.upsert('patients', patient)

        last_patient_id = patient['id']

        
        ['medical_certificates', 'patient_registration_summaries', 'patient_extra_fields', 'patient_tags', 'patient_private_notes', 'patient_vital_infos'].each do |table|
          BackupReader.fetch(table, { patient_id: patient['id'] }).each do |record|
            MainDbWriter.upsert(table, record)
          end
        end
      end
    end
  end
end