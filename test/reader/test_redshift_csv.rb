require 'test/unit'
require 'redshift_connector/data_file'

module RedshiftConnector
  module Reader
    class TestRedshiftCSV < Test::Unit::TestCase
      def parse_row(line)
        r = RedshiftCSV.new(StringIO.new(line))
        r.read_row
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

      def test_each_row
        r = RedshiftCSV.new(StringIO.new(%Q("95"\n)))
        assert_equal ['95'], r.read_row
        assert_nil r.read_row
      end
    end
  end
end
