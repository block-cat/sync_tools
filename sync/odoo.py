import odoorpc

class OdooReader(object):

    def __init__(self,host,port,db,user,password):
        self.odoo = odoorpc.ODOO(host=host,port=port,protocol='jsonrpc+ssl')
        self.odoo.login(db,user,password)

    def get_cmb_shops(self):
        return self.odoo.env['shop.store'].search([('corporator','=','shyd')])