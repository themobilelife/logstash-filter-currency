# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "open-uri"
require "date"

class LogStash::Filters::Currency < LogStash::Filters::Base

  config_name "currency"
  config :fields, :validate => :array
  config :api_address, :validate => :string

  public
  def register
    @fx = {}
  end

  public
  def filter(event)

    dateStr = event.get('date')
    currencies = event.get('[@metadata][quote_currency]')

    begin
      date = Date.parse(dateStr)
    # This should NOT happen as these events are violating the schema.
    # But until we can catch them earlier in the process (middleware for Firehose maybe?).
    # We must deal with them here..
    rescue TypeError
      @logger.error("(ERROR) Failed to parse #{dateStr} as a date!")
      event.cancel
      return
    end

    # Because USD is always the base currency we must first get the base currency rate against USD
    # For example if we wanted to do conversion SEK/EUR we would first get USD/SEK rate.
    base_to_usd_rate = get_rate(event.get('currency'), date)

    @fields.each do |field|
      unless event.include?(field)
        raise "The received event does not contain required key, #{field}!"
      end

      field_name = "converted" + field.capitalize
      converted_amounts = Hash[currencies.split(",").map do |x|
          # Calculate quote currency rate against USD
          quote_to_usd_rate = get_rate(x, date)

          # Finally convert the amounts by first dividing the amount with the USD/BASE rate and multiplying that by the USD/QUOTE rate.
          amount = event.get(field) / base_to_usd_rate * quote_to_usd_rate
          [x, amount]
        end]

      event.set(field_name, converted_amounts)
    end
    filter_matched(event)
  end

  public
  def get_rate(currency, date)

    # Format date to DD/MM/YYYY
    date_str = date.strftime("%Y-%m-%d")

    # Check if we have the rates for this date memoized first
    if @fx.key?(date_str) && @fx[date_str]["rates"].key?(currency)
      return @fx[date_str]["rates"][currency]
    else
      # Memoize the results
      url = "http://#{@api_address}/rates/#{date_str}"
      begin
        @fx[date_str] = JSON.load(open(url))
      rescue OpenURI::HTTPError
        # Get dates from day before
        return get_rate(currency, date - 1)
      end
      if @fx[date_str]["rates"].key?(currency)
        return @fx[date_str]["rates"][currency]
      end
    end
    # If we reach this point then we were unable to find the requested currency
    raise "Rates for #{currency} (#{date_str}) could not be found!"
  end
end
