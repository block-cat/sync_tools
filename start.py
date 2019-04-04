
from config import Config
from sync.mysql_sync import MysqlSync
from utils import config, odoo_config, logger
from sync.odoo import OdooReader


shop_ids = tuple(OdooReader(odoo_config.host, odoo_config.port,
                            odoo_config.db, odoo_config.user, odoo_config.password).get_cmb_shops())

# shop_ids = [7, 66]

shop_ids = f"({shop_ids[0]})" if len(shop_ids) == 1 else tuple(shop_ids)

logger.info(f"待同步的店铺列表：{shop_ids},\n共{len(shop_ids)}个店铺")

mysync = MysqlSync(config.src_host, config.src_port,
                   config.src_user, config.src_password,
                   config.src_db, config.dest_host,
                   config.dest_port, config.dest_user,
                   config.dest_password, config.dest_db)

details = mysync.get_query_data(
    f"select id from in_storage where shop_id in {shop_ids}")
storage_ids = tuple([d[0] for d in details])

tables = [
    ("repertory", f"shop_id in {shop_ids}"),
    ("in_storage", f"shop_id in {shop_ids}"),
    ("storage_detail",
     f"storage_id in {storage_ids}" if storage_ids else '1!=1'),
    ("shop_order", f"store_id in {shop_ids}"),
    ("shop_sales_records", f"store_id in {shop_ids}"),
    ("out_storage", f"shop_id in {shop_ids}"),
]


mysync.sync_table(tables)
