
from config import Config
from sync.mysql_sync import MysqlSync

config = Config('config.ini', "MYSQL")

shop_ids = 7

tables = [
    ("repertory", f"shop_id in ({shop_ids})"),
    ("in_storage", f"shop_id in ({shop_ids})"),
    ("storage_detail", f"shop_id in ({shop_ids})"),
    ("shop_order", f"store_id in ({shop_ids})"),
    ("shop_sales_record", f"store_id in ({shop_ids})"),
    ("out_storage", f"store_id in ({shop_ids})"),
]

mysync = MysqlSync(config.src_host, config.src_port,
                   config.src_user, config.src_password,
                   config.src_db, config.dest_host,
                   config.dest_port, config.dest_user,
                   config.dest_password, config.dest_db)

mysync.sync_table(tables)
