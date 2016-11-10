# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "open-uri"
require "date"

class LogStash::Filters::Currency < LogStash::Filters::Base

  config_name "currency"

  config :currency, :validate => :string
  config :fields, :validate => :array

  public
  def register
    @fx = {}

    if ENV["CURRENCY_SERVICE_HOST"].nil?
      raise "Required environment variable CURRENCY_SERVICE_HOST was not set!"
    end

    @api_addr = ENV["CURRENCY_SERVICE_HOST"]
  end

  public
  def filter(event)

    @fields.each do |field|
      d = Date.parse(event['date'])
      # Format date to DD/MM/YYYY
      date = d.strftime("%Y-%m-%d")

      unless field in event
        raise "The received event does not contain required key, #{field}!"
      end

      # Because USD is always the base currency we must first get the base currency rate against USD
      # For example if we wanted to do conversion SEK/EUR we would first get USD/SEK rate.
      base_to_usd_rate = getRate(event['currency'], date)

      # Then follow that up with the quote currency rate against USD
      quote_to_usd_rate = getRate(@currency, date)

      # Finally convert the amounts by first dividing the amount with the USD/BASE rate and multiplying that by the USD/QUOTE rate.
      amount = event[field] / base_to_usd_rate * quote_to_usd_rate
      event["converted" + field.capitalize] = amount
    end

    filter_matched(event)
  end

  public
  def getRate(currency, date)
    # Check if we have the rates for this date memoized first
    if @fx.key?(date) && @fx[date]["rates"].key?(currency)
      return @fx[date]["rates"][currency]
    else
      # Memoize the results
      url = "http://#{@api_addr}/rates/#{date}"
      @fx[date] = JSON.load(open(url))
      if @fx[date]["rates"].key?(currency)
        return @fx[date]["rates"][currency]
      end
    end
    # If we reach this point then we were unable to find the requested currency
    raise "Rates for #{currency} could not be found!"
  end
end
