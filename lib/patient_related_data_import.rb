class PatientRelatedDataImport

  def self.import(venue_id)
    patient_count = BackupReader.count('patients', { venue_id: venue_id })

    last_patient_id = 0

    batch_size = 10
    loops = (patient_count.to_f / batch_size).ceil

    (0..loops).each do |loop|
      puts "Processing loop #{loop + 1} of #{loops}"
      patients = BackupReader.fetch('patients', { venue_id: venue_id }, { id: last_patient_id }, batch_size)

      if patients.blank?
        break
      end

      patients.each do |patient|
        MainDbWriter.upsert('patients', patient)

        last_patient_id = patient['id']

        ['patient_extra_fields'].each do |table|
          BackupReader.fetch(table, { patient_id: patient['id'] }).each do |record|
            MainDbWriter.upsert(table, record)
          end
        end
      end
    end
  end
end