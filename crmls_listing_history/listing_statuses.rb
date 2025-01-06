    def perchwell_status_code(status = self.standard_status)
      case Reso::ResoProperty.standard_statuses[status]
      when "Pending", "Active Under Contract"
        240
      when "Closed"
        if self.rental?
          400
        else
          500
        end
      when "Withdrawn", "Canceled", "Delete"
        600
      when "Incomplete"
        999
      when "Hold"
        640 # TOM
      when "Active"
        100
      ## Note: according to the RESO definition, this maps to "Accepted Offer"
      ## but Rebny maps it to In-Contract
      # when "Active Under Contract"
      #   200
      when "Expired"
        300
      when "Incomplete"
        999
      when "Coming Soon"
        50
      end
    end