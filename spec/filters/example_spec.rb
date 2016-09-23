# encoding: utf-8
require 'spec_helper'
require "logstash/filters/currency"

describe LogStash::Filters::Currency do
  describe "Set to Hello World" do
    let(:config) do <<-CONFIG
      filter {
        currency {
          currency => "SGD"
        }
      }
    CONFIG
    end

    sample("currency" => "SGD") do
      expect(subject).to include("currency")
      expect(subject['currency']).to eq('SGD')
    end
  end
end
