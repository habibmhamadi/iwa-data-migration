import xmlrpc.client
from psycopg2 import connect
from config.env import DB_PARAMS, O_HOST, O_PORT, O_DB, O_USER, O_PWD


def init_connection():
    try:
        db = connect(**DB_PARAMS)
        cr = db.cursor()
        common = xmlrpc.client.ServerProxy(f'{O_HOST}:{O_PORT}/xmlrpc/2/common')
        uid = common.authenticate(O_DB, O_USER, O_PWD, {})
        models =  xmlrpc.client.ServerProxy(f'{O_HOST}:{O_PORT}/xmlrpc/2/object')
        print('**** Connected Successfully ****')
        return db, cr, models, uid
    except Exception as e:
        print('\n***** ERROR Connecting to database or Odoo *****', e)
