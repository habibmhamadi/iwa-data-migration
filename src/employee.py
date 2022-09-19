from config.db import init_connection


db, cr, models, uid = init_connection()

cr.execute("""
    SELECT 
        id,
        login,
        password,
        active,
        signature
    FROM
        res_users
    WHERE
        login LIKE '%@%'
    ORDER BY
        id
""")

db_users = cr.fetchall()
print('\n', db_users)