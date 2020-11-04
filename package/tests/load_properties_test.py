import unittest

from AivenHW.Properties import load_properties

class TestLoadProperties(unittest.TestCase):
    """Tests for PropertiesLoader class"""

    def setUp(self) -> None:
        self.test_properties_file = "load_properties_test.properties"

    def test_load_properties(self):
        properties = load_properties(self.test_properties_file)

        self.assertDictEqual(properties, {'name' : 'Anantha',
                                          'file' : 'abcd/xyz/123',
                                          'time' : '3 Nov 2020 17:15:46',
                                          'email' : 'anantha@email.com'})


# __name__ == __main__
#unittest.main(verbosity=2)