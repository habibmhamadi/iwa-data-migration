import xmlrpc.client
from psycopg2 import connect
from config.env import DB_PARAMS, O_HOST, O_PORT, O_DB, O_USER, O_PWD


def init_connection():
    try:
        db = connect(**DB_PARAMS)
        cr = db.cursor()
        common = xmlrpc.client.ServerProxy(f'{O_HOST}:{O_PORT}/xmlrpc/2/common', allow_none=True)
        O_UID = common.authenticate(O_DB, O_USER, O_PWD, {})
        odoo =  xmlrpc.client.ServerProxy(f'{O_HOST}:{O_PORT}/xmlrpc/2/object', allow_none=True)
        print('**** Connected Successfully ****')
        return db, cr, odoo, O_DB, O_UID, O_PWD
    except Exception as e:
        print('\n***** ERROR Connecting to database or Odoo *****', e)
