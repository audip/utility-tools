import unittest
import application

class ApplicationTests(unittest.TestCase):

    def setUp(self):
        self.es_client = application.connect_elasticsearch(
                        host='elasticsearch-url',
                        scheme='https',
                        port=443)

    def tearDown(self):
        pass

    def test_connect_elasticsearch(self):
        self.assertIsNotNone(self.es_client)

    def test_get_all_indices(self):
        indices_list = application.get_all_indices(self.es_client)
        self.assertIsNotNone(indices_list[0]['index'])
        self.assertIsNotNone(indices_list[0]['docs.count'])
        self.assertIsNotNone(indices_list[0]['store.size'])
        self.assertTrue(len(indices_list)!=0)
