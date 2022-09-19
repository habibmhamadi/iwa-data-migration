from config.db import init_connection


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()


# COMPANY
cr.execute("""
    SELECT 
        name,
        email,
        phone
    FROM
        res_company
    WHERE
        id = 1""")

company = cr.fetchone()
company = {'name': company[0], 'email': company[1], 'phone': company[2]}


# COMPANY CONTACT
cr.execute("""
    SELECT 
        name,
        company_id,
        website,
        street,
        zip,
        city,
        country_id,
        email,
        phone,
        commercial_company_name,
        email_normalized,
        phone_sanitized
    FROM
        res_partner
    WHERE
        id = 1 """)

company_contact = cr.fetchone()
company_contact = {
    'name': company_contact[0],
    'company_id': company_contact[1],
    'website': company_contact[2],
    'street': company_contact[3],
    'zip': company_contact[4],
    'city': company_contact[5],
    'country_id': company_contact[6],
    'email': company_contact[7],
    'phone': company_contact[8],
    'commercial_company_name': company_contact[9],
    'email_normalized': company_contact[10],
    'phone_sanitized': company_contact[11],
}


# HR JOB
cr.execute("""
    SELECT
        id,
        name
    FROM
        hr_job""")

jobs = [{'id': job[0], 'name':job[1]} for job in cr.fetchall()]


# HR Department
cr.execute("""
    SELECT
        id,
        name,
        active,
        parent_id
    FROM
        hr_department""")

departments = [{'id': dep[0], 'name':dep[1],  'active':dep[2], 'parent_id':dep[3]} for dep in cr.fetchall()]


# HR Employee
cr.execute("""
    SELECT
        id,
        name,
        address_id,
        coach_id,
        company_id,
        department_id,
        job_id,
        parent_id,
        work_phone,
        mobile_phone,
        work_email,
        idc_no,
        personal_mobile,
        personal_email,
        resource_calendar_id,
        country_id,
        active,
        gender,
        marital,
        birthday,
        place_of_birth,
        identification_id,
        certificate,
        km_home_work,
        color,
        tin_no,
        father_name,
        grand_father_name,
        current_address,
        permanent_address,
        hr_bank_account
    FROM
        hr_employee
    WHERE
        user_id IS NULL
    OR
        user_id > 5""")

employees = [{
    'id': emp[0],
    'name': emp[1],
    'address_id': emp[2],
    'coach_id': emp[3],
    'company_id': emp[4],
    'department_id': emp[5],
    'job_id': emp[6],
    'parent_id': emp[7],
    'work_phone': emp[8],
    'mobile_phone': emp[9],
    'work_email': emp[10],
    'idc_no': emp[11],
    'personal_mobile': emp[12],
    'personal_email': emp[13],
    'resource_calendar_id': emp[14],
    'country_id': emp[15],
    'active': emp[16],
    'gender': emp[17],
    'marital': emp[18],
    'birthday': emp[19],
    'place_of_birth': emp[20],
    'identification_id': emp[21],
    'certificate': emp[22],
    'km_home_work': emp[23],
    'color': emp[24],
    'tin_no': emp[25],
    'father_name': emp[26],
    'grand_father_name': emp[27],
    'current_address': emp[28],
    'permanent_address': emp[29],
    'hr_bank_account': emp[30]
} for emp in cr.fetchall()]

print('\n', company, '\n\n', company_contact, jobs, departments, employees)



# ids = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.users', 'create', [db_users])
# print('\n*** Users Inserted ****')

