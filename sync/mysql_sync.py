
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

    def sync_specify(self, shop_ids):
        """同步特殊表"""
        # shop_order表
        try:
            olds = "select order_id,store_id,member_id,create_date,write_date,payed_total,receivable \
                small_change, discount,erase,pay_type,status,operator_id,day_order_seq,payment_scenarios,\
                    payment_channel from shop_order where store_id in {shop_ids}"
            self.src_cr.execute(olds)
            data = self.src_cr.fetchall()
            for row in data:
                insert_sql = f"replace into shop_order (order_id,store_id,member_id,create_date,write_date,amount_payed, amount_receivable, \
                                odd_change, \
                                discount_amount, \
                                erase,\
                                pay_type,\
                                status,\
                                operator_id,\
                                day_order_seq,\
                                payment_scenarios,\
                                payment_channel) values ({','.join( '%s' for _ in range(16))})"
                self.dest_cr.execute(insert_sql, row)

            self.dest_conn.commit()

            detail_sql = "select id,count,create_date,product_id,order_id,sales_status,operator,pay_type,name,operator_name,category,store_id,retail_price,trade_price,price_per, write_date\
                from shop_sales_records"
            self.src_cr.execute(detail_sql)
            data = self.src_cr.fetchall()
            for row in data:
                inser_sql = f"replace into shop_sales_records (id,quantity,create_date,goods_code,order_id,sales_status,operator,pay_type,goods_name,operator_name,category,\
                    store_id,retail_price,trade_price,sale_price,write_date) values ({','.join( '%s' for _ in range(16))})"
                
                self.dest_cr.execute(inser_sql, row)

            self.dest_conn.commit()
        except Exception as err:
            self.dest_conn.rollback()
            logger.error(f"订单表或订单详情表异常：{traceback.format_exc()}")
