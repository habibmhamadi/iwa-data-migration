from odoorpc import ODOO
from psycopg2 import connect
from config.env import DB_PARAMS, O_HOST, O_PORT, O_DB, O_USER, O_PWD


def init_connection():
    try:
        db = connect(**DB_PARAMS)
        cr = db.cursor()
        odoo = ODOO(O_HOST, port=O_PORT)
        odoo.login(O_DB, O_USER, O_PWD)
        print('**** Connected Successfully ****')
        return db, cr, odoo
    except Exception as e:
        print('\n***** ERROR Connecting to database or Odoo *****', e)
