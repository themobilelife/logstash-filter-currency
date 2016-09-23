# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "json"

# This example filter will replace the contents of the default 
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an example.
class LogStash::Filters::Currency < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.

  config_name "currency"
  
  # Replace the message with this value.
  config :currency, :validate => :string, :default => "" 
  config :fields, :validate => :array, :default => ""   

  public
  def register
    # Add instance variables 
    @currencies = JSON.parse(File.read('../currency_json/currency.json'))     
  end # def register

  public
  def filter(event)    

    if @currency and event["currency"] and @fields      
      
      # pprice: "1.401070"
      @fields.each_with_index do |element,index|
        # Concat USD with airline currency 

        rate = getRate("USD/" + event["currency"])

        # Get airline USD currency rate?
        rate2 = getRate("USD/" + @currency)
        event["converted" + @fields[index].capitalize] = (event[@fields[index]].to_f / (rate.to_f/rate2.to_f))           
      end  

      # event["convertedAmount"] = (event["amount"].to_f / (rate.to_f/rate2.to_f))            
      event["convertedCurrency"] = @currency
    end     

    filter_matched(event)

  end 

  public 
  def getRate(currency)
      arr = @currencies["list"]["resources"]   

      arr.each_with_index do |element,index| 
          if (arr[index]["resource"]["fields"]["name"] == currency)
            price = arr[index]["resource"]["fields"]["price"] 
            return price  
          end 
      end 

      return 1  
  end
end # class LogStash::Filters::Example
