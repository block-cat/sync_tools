import unittest
from sync.odoo import OdooReader
from utils import odoo_config


class TestOdoo(unittest.TestCase):

    def test_get_stores(self):
        reader = OdooReader(odoo_config.host, odoo_config.port,
                            odoo_config.db, odoo_config.user, odoo_config.password)
        self.assertTrue(len(reader.get_cmb_shops()))


if __name__ == "__main__":
    unittest.main()
