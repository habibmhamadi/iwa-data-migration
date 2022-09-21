from config.db import init_connection


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()

archieved_condition = ['|', ['active', '=', True], ['active', '=', False]]


def insert_1():

    cr.execute("""
        SELECT
            id,
            name,
            active,
            employee_id,
            date_start::TEXT,
            date_end::TEXT,
            trial_date_end::TEXT,
            company_id,
            kanban_state,
            date_generated_from::date::TEXT,
            date_generated_to::date::TEXT,
            vacancy_number,
            duration,
            job_summary,
            legal_leave,
            sick_leave,
            personal_leave,
            transportation,
            house_allowance,
            food_allowance,
            house_allowance_figure,
            site_lunch_allowance,
            wage_afn,
            transportation_afn,
            grade,
            step,
            duty_station,
            wage,
            state,
            contract_signatory,
            structure_type_id,
            department_id,
            job_id
        FROM
            hr_contract
        ORDER BY
            id
    """)

    db_contracts = [{
        'old_id': con[0],
        'name': con[1],
        'active': con[2],
        'employee_id': con[3],
        'date_start': con[4],
        'date_end': con[5],
        'trial_date_end': con[6],
        'company_id': con[7],
        'kanban_state': con[8],
        'date_generated_from': con[9],
        'date_generated_to': con[10],
        'vacancy_number': con[11],
        'duration': con[12],
        'job_summary': con[13],
        'legal_leave': con[14],
        'sick_leave': con[15],
        'personal_leave': con[16],
        'transportation': con[17],
        'house_allowance': con[18],
        'food_allowance': con[19],
        'house_allowance_figure': con[20],
        'site_lunch_allowance': con[21],
        'wage_afn': con[22],
        'transportation_afn': con[23],
        'grade': con[24],
        'step': con[25],
        'duty_station': con[26],
        'wage': con[27],
        'state': con[28],
        'contract_signatory': con[29],
        'structure_type_id': con[30],
        'department_id': con[31],
        'job_id': con[32],
        'contract_approver': 2,

    } for con in cr.fetchall()]

    for index, con in enumerate(db_contracts):
        emp_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', con.get('employee_id')], *archieved_condition]])
        if emp_id:
            con.update({'employee_id': emp_id[0]})
            
            signatory = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', con.get('contract_signatory')], *archieved_condition]])
            con.update({'contract_signatory': signatory and signatory[0] or False})

            dep = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', con.get('department_id')], *archieved_condition]])
            con.update({'department_id': dep and dep[0] or False})

            job = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.job', 'search', [[['old_id', '=', con.get('job_id')]]])
            con.update({'job_id': job and job[0] or False})
            
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.contract', 'create', [con])
            print(f'{index+1}/{len(db_contracts)}')


insert_1()