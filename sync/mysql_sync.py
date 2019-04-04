
import mysql.connector
from utils import logger
import traceback


class MysqlSync(object):

    def __init__(self, src_host, src_port, src_user, src_password, src_db, dest_host, dest_port, dest_user, dest_password, dest_db):
        self.src_conn = mysql.connector.connect(
            host=src_host,
            port=src_port,
            user=src_user,
            passwd=src_password,
            database=src_db
        )
        self.src_cr = self.src_conn.cursor()

        self.dest_conn = mysql.connector.connect(
            host=dest_host,
            port=dest_port,
            user=dest_user,
            passwd=dest_password,
            database=dest_db
        )

        self.dest_cr = self.dest_conn.cursor()

    def get_query_data(self, sql):
        """获取单条sql查询结果"""
        try:
            self.src_cr.execute(sql)
            return self.src_cr.fetchall()
        except Exception as err:
            logger.error(f"查询错误：{traceback.format_exc()}")

    def sync_table(self, tables):
        """同步数据库表"""
        # 获取表字段
        try:
            for table, condtion in tables:
                logger.info(f"正在同步{table}表...")
                sql = "SHOW columns FROM {}".format(table)
                self.src_cr.execute(sql)
                columns = self.src_cr.fetchall()
                keys = [col[0] for col in columns]
                select_ql = f"select {','.join(keys)} from {table} where {condtion}"
                self.src_cr.execute(select_ql)
                data = self.src_cr.fetchall()
                for row in data:
                    insert_sql = f"REPLACE  into {table} ({','.join(keys)}) values ({','.join( '%s' for _ in range(len(keys)))})"
                    # logger.info(insert_sql, row)
                    self.dest_cr.execute(insert_sql, row)

                self.dest_conn.commit()
        except Exception as ex:
            self.dest_conn.rollback()
            logger.error(f"同步数据库出错：{traceback.format_exc()}")
