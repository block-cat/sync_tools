
import mysql.connector


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

    def sync_table(self, tables):
        """同步数据库表"""
        # 获取表字段
        for table, condtion in tables:
            sql = "SHOW columns FROM {}".format(table)
            self.src_cr.execute(sql)
            columns = self.src_cr.fetchall()
            keys = [col[0] for col in columns]
            select_ql = f"select {','.join(keys)} from {table} where {condtion}"
            self.src_cr.execute(select_ql)
            data = self.src_cr.fetchall()
            for row in data:
                insert_sql = f"REPLACE  into {table} ({','.join(keys)}) values ({','.join( '%s' for _ in range(len(keys)))})"
                # print(insert_sql)
                self.dest_cr.execute(insert_sql, row)

            self.dest_conn.commit()
