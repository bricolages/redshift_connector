require 'test/unit'
require 'redshift-connector/data_file'

module RedshiftConnector
  module Reader
    class TestRedshiftCSV < Test::Unit::TestCase
      def parse_row(line)
        r = RedshiftCSV.new(nil)
        r.parse_row(line, 1)
      end

      def test_parse_row
        assert_equal ['xxx', 'yyyy', 'zzz'],
          parse_row(%Q("xxx","yyyy","zzz"\n))

        assert_equal ['xxx', 'yyyy', 'zzz'],
          parse_row(%Q( "xxx" , "yyyy","zzz"\t\n))

        assert_equal ['x,x', "y\r\ny", 'z"z', 'a\\a'],
          parse_row(%Q("x\\,x","y\\r\\ny","z\\"z","a\\\\a"\n))

        assert_equal ['981179', '2017-01-07', '6', 'show', '99', '3'],
          parse_row(%Q("981179","2017-01-07","6","show","99","3"\r\n))

        assert_equal ['981179', '2017-01-07', '6', '852', 'show', '{"page"=>"4"}', '1', '1'],
          parse_row(%Q("981179","2017-01-07","6","852","show","{\\"page\\"=>\\"4\\"}","1","1"\n))
      end
    end
  end
end
