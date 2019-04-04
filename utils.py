from autils import Logger
logger = Logger("sync.log").logger


from config import Config
config = Config('config.ini', "MYSQL")

odoo_config = Config('config.ini',"ODOO")