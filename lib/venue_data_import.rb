class VenueDataImport

  def self.import
  	venue_record = "974638072"

  	PatientRelatedDataImport.import(venue_record)
  	PrescriptionRelatedDataImport.import(venue_record)
  	ReportDispatchRelatedDataImport.import(venue_record)
  	BillRelatedDataImport.import(venue_record)
  end

end