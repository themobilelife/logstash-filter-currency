# encoding: utf-8
require 'spec_helper'
require "logstash/filters/currency"

describe LogStash::Filters::Currency do
  describe "Set to USD and HKD" do
    config <<-CONFIG
      filter {
        currency {
          currency => "USD,HKD"
          fields => ["amount"]
          api_address => "34.250.6.139:8080"
        }
      }
    CONFIG

    sample("currency" => "SGD", "date" => "2014-01-01", "amount" => 235) do
      insist { subject.get("convertedAmount") }.key? "USD"
      insist { subject.get("convertedAmount") }.key? "HKD"
    end

  end
end
