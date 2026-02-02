class BillRelatedDataImport
  
  def self.import(venue_id)
    bill_count = BackupReader.count('bills', { venue_id: venue_id })

    last_bill_id = 0

    batch_size = 10
    loops = (bill_count.to_f / batch_size).ceil

    (0..loops).each do |loop|
      puts "Processing loop #{loop + 1} of #{loops}"
      bills = BackupReader.fetch('bills', { venue_id: venue_id }, { id: last_bill_id }, batch_size)

      if bills.blank?
        break
      end

      bills.each do |bill|
        MainDbWriter.upsert('bills', bill)

        last_bill_id = bill['id']

        ['bill_items', 'bill_paid_details', 'bill_refund_details', 'credit_bill_notes', 'billing_coupons', 'bill_extra_fields'].each do |bill_has_table|
          BackupReader.fetch(bill_has_table, { bill_id: bill['id'] }).each do |record|
            MainDbWriter.upsert(bill_has_table, record)

            if bill_has_table == 'bill_items'
              ['bill_item_extra_fields'].each do |bill_item_has_table|
                BackupReader.fetch(bill_item_has_table, { bill_item_id: record['id'] }).each do |bill_item_has_record|
                   MainDbWriter.upsert(bill_item_has_table, bill_item_has_record)
                end
              end
            end
            
            if bill_has_table == 'bill_paid_details'
              ['payment_cancel_reasons', 'bill_paid_detail_wallets'].each do |bill_paid_detail_has_table|
                BackupReader.fetch(bill_paid_detail_has_table, { bill_paid_detail_id: record['id'] }).each do |bill_paid_detail_has_record|
                   MainDbWriter.upsert(bill_paid_detail_has_table, bill_paid_detail_has_record)
                end
              end
            end


          end
        end
      end
    end
  end

end