# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "open-uri"
require "date"

class LogStash::Filters::Currency < LogStash::Filters::Base

  config_name "currency"

  config :currency, :validate => :array
  config :fields, :validate => :array
  config :api_address, :validate => :string

  public
  def register
    @fx = {}
  end

  public
  def filter(event)
    d = Date.parse(event.get('date'))

    # Format date to DD/MM/YYYY
    date = d.strftime("%Y-%m-%d")

    # Because USD is always the base currency we must first get the base currency rate against USD
    # For example if we wanted to do conversion SEK/EUR we would first get USD/SEK rate.
    base_to_usd_rate = get_rate(event.get('currency'), date)

    @fields.each do |field|
      unless event.include?(field)
        raise "The received event does not contain required key, #{field}!"
      end

      field_name = "converted" + field.capitalize
      event.set('[field_name]', {})

      @currency.each do |currency|
        # Calculate quote currency rate against USD
        quote_to_usd_rate = get_rate(currency, date)

        # Finally convert the amounts by first dividing the amount with the USD/BASE rate and multiplying that by the USD/QUOTE rate.
        amount = event.get(field) / base_to_usd_rate * quote_to_usd_rate
        event.set("[#{field_name}][#{currency}]", amount)
      end
    end

    filter_matched(event)
  end

  public
  def get_rate(currency, date)
    # Check if we have the rates for this date memoized first
    if @fx.key?(date) && @fx[date]["rates"].key?(currency)
      return @fx[date]["rates"][currency]
    else
      # Memoize the results
      url = "http://#{@api_address}/rates/#{date}"
      begin
        @fx[date] = JSON.load(open(url))
      rescue OpenURI::HTTPError => e
        if e.message == '404 Not Found'
          # Get dates from day before
          url = "http://#{@api_address}/rates/#{date - 1}"
          @fx[date] = JSON.load(open(url))
        else
          raise e
        end
      end
      if @fx[date]["rates"].key?(currency)
        return @fx[date]["rates"][currency]
      end
    end
    # If we reach this point then we were unable to find the requested currency
    raise "Rates for #{currency} (#{date}) could not be found!"
  end
end
