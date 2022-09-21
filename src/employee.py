from config.db import init_connection
import base64


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()

archieved_condition = ['|', ['active', '=', True], ['active', '=', False]]


# HR Department
def get_departments():
    cr.execute("""
        SELECT
            id,
            name,
            active,
            parent_id,
            manager_id
        FROM
            hr_department
        WHERE
            id > 2
        ORDER BY
            parent_id NULLS FIRST""")
    return [{'old_id': dep[0], 'name':dep[1], 'active':dep[2]} for dep in cr.fetchall()]


# HR Employee
def get_employees():
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
            birthday::TEXT,
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
            id > 1
        ORDER BY
            parent_id, coach_id NULLS FIRST""")

    return [{
        'old_id': emp[0],
        'name': emp[1],
        'address_id': emp[2],
        'company_id': emp[4],
        'department_id': emp[5],
        'job_id': emp[6],
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




def insert_part_1():

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

    company_partner = cr.fetchone()
    company_partner = {
        'name': company_partner[0],
        'company_id': company_partner[1],
        'website': company_partner[2],
        'street': company_partner[3],
        'zip': company_partner[4],
        'city': company_partner[5],
        'country_id': company_partner[6],
        'email': company_partner[7],
        'phone': company_partner[8],
        'commercial_company_name': company_partner[9],
        'email_normalized': company_partner[10],
        'phone_sanitized': company_partner[11],
    }

    # HR JOB
    cr.execute("""
        SELECT
            id,
            name
        FROM
            hr_job""")

    jobs = [{'old_id': job[0], 'name':job[1]} for job in cr.fetchall()]

    odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.company', 'write', [[1], company])
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'write', [[1], company_partner])
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.job', 'create', [jobs])
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'create', [get_departments()])
    print('*** Migration 1 Success ***')


def insert_part_2():
    emps = get_employees()
    for emp in emps:
        if emp.get('department_id'):
            department_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', emp.get('department_id')], *archieved_condition]])
            if department_id:
                emp.update({'department_id': department_id[0]})
        if emp.get('job_id'):
            job_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.job', 'search', [[['old_id', '=', emp.get('job_id')]]])
            if job_id:
                emp.update({'job_id': job_id[0]})
        if emp.get('address_id'):
            if not odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'search', [[['id', '=', emp.get('address_id')], *archieved_condition]]):
                emp.update({'address_id': None})
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'create', [emps])
    print('*** Migration 2 Success ***')
    

def insert_part_3():
    deps = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search_read', [[['old_id', 'in', [dep.get('old_id') for dep in get_departments()]], *archieved_condition]])
    for index, dep in enumerate(deps):
        cr.execute("SELECT manager_id, parent_id FROM hr_department WHERE id = %s", (dep.get('old_id'),))
        dep_data = cr.fetchone()
        new_manager_id = False
        new_parent_id = False
        if dep_data and dep_data[0]:
            new_manager_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', dep_data[0]], *archieved_condition]])
            if new_manager_id:
                new_manager_id = new_manager_id[0]
        if dep_data and dep_data[1]:
            new_parent_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', dep_data[1]], *archieved_condition]])
            if new_parent_id:
                new_parent_id = new_parent_id[0]
        odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'write', [[dep.get('id')], {'manager_id': new_manager_id, 'parent_id': new_parent_id}])
        print(f'{index+1}/{len(deps)}')
        

    emps = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search_read', [[['old_id', 'in', [emp.get('old_id') for emp in get_employees()]], *archieved_condition]])
    for index, emp in enumerate(emps):
        cr.execute("SELECT coach_id, parent_id FROM hr_employee WHERE id = %s", (emp.get('old_id'),))
        emp_data = cr.fetchone()
        new_coach_id = False
        new_parent_id = False
        if emp_data and emp_data[0]:
            new_coach_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp_data[0]], *archieved_condition]])
            if new_coach_id:
                new_coach_id = new_coach_id[0]
        if emp_data and emp_data[1]:
            new_parent_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp_data[1]], *archieved_condition]])
            if new_parent_id:
                new_parent_id = new_parent_id[0]
        odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'write', [[emp.get('id')], {'coach_id': new_coach_id, 'parent_id': new_parent_id}])
        print(f'{index+1}/{len(emps)}')
    print('*** Migration 3 Success ***')
    

    
def insert_part_4():

    # Emergency Contact
    cr.execute("""
        SELECT
            contacts,
            json_agg(
                json_build_object(
                    'name', name,
                    'relationship', relationship,
                    'number', number
                )
            )
        FROM
            emergency_contacts
        GROUP BY
            contacts""")
    emergency_contacts = [{'old_emp_id': emg[0], 'contacts': [{
                            'name': contact.get('name'),
                            'relationship': contact.get('relationship'),
                            'number': contact.get('number'),
                          } for contact in emg[1]]} for emg in cr.fetchall()]


    for emp in emergency_contacts:
        if emp.get('contacts'):
            emp_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp.get('old_emp_id')], *archieved_condition]])
            if emp_id:
                contacts = list(map(lambda x: {**x, **{'contact_id': emp_id[0]}}, emp.get('contacts')))
                odoo.execute_kw(O_DB, O_UID, O_PWD, 'emergency.contact', 'create', [contacts])


    # Employee Reference
    cr.execute("""
        SELECT
            empl_id,
            json_agg(
                json_build_object(
                    'name', name,
                    'job_title', job_title,
                    'organization', organization,
                    'contact_number', contact_number,
                    'email', email,
                    'checked', checked
                )
            )
        FROM
            employee_references
        GROUP BY
            empl_id""")
    emp_references = [{'old_emp_id': emg[0], 'references': [{
                            'name': ref.get('name'),
                            'job_title': ref.get('job_title'),
                            'organization': ref.get('organization'),
                            'contact_number': ref.get('contact_number'),
                            'email': ref.get('email'),
                            'checked': ref.get('checked'),
                          } for ref in emg[1]]} for emg in cr.fetchall()]

    for emp in emp_references:
        if emp.get('references'):
            emp_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp.get('old_emp_id')], *archieved_condition]])
            if emp_id:
                refs = list(map(lambda x: {**x, **{'employee_id': emp_id[0]}} , emp.get('references')))
                odoo.execute_kw(O_DB, O_UID, O_PWD, 'employee.reference', 'create', [refs])


    print('*** Migration 4 Success ***')
    

def insert_part_5():
    emp_ids = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search_read', [[*archieved_condition]])
    for index, emp in enumerate(emp_ids):
        cr.execute("SELECT store_fname FROM ir_attachment WHERE res_model = 'hr.employee' AND res_id = %s AND res_field = 'image_1920' LIMIT 1", (emp.get('old_id'),))
        f_name = cr.fetchone()
        byte_data = None
        if f_name:
            try:
                file = open(f'filestore/{f_name[0]}', "rb")
                byte_data = base64.b64encode(file.read()).decode('utf-8')
            except Exception as e:
                print(f"ERROR reading attachment for employee {emp.get('id')}", e)
        if byte_data:
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'write', [[emp.get('id')], {'image_1920': byte_data}])
        print(f'{index+1}/{len(emp_ids)}')
    print('*** Migration 5 Success ***')


def insert_part_6():
    cr.execute("""
        SELECT 
            doc.emp_id,
            json_agg(json_build_object('name', doc.file_name, 'file', att.store_fname))
        FROM 
            employee_document doc INNER JOIN ir_attachment att ON att.res_id = doc.id
        WHERE
            att.res_model = 'employee.document'
        AND
            att.res_field = 'name'
        GROUP BY
            doc.emp_id
    """)
    counter = 0
    for rec in cr.fetchall():
        emp = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search_read', [[['old_id', '=', rec[0]], *archieved_condition]])
        if emp:
            emp = emp[0]
            for doc in rec[1]:
                byte_data = None
                try:
                    file = open(f"filestore/{doc.get('file')}", "rb")
                    byte_data = base64.b64encode(file.read()).decode('utf-8')
                    odoo.execute_kw(O_DB, O_UID, O_PWD, 'employee.document', 'create', [{'employee_id': emp.get('id'), 'name': doc.get('name'), 'document': byte_data}])
                except Exception as e:
                    print(f"ERROR reading attachment for employee {emp.get('id')}", doc.get('name'))
                print(f'{counter}')
                counter+=1
    
    print('*** Migration 6 Success ***')


# Run each one separately (All others should be commented each time)

# insert_part_1()
# insert_part_2()
# insert_part_3()
# insert_part_4()
# insert_part_5()
# insert_part_6()

