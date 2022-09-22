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
            job_id,
            analytic_account_id
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
        'analytic_account_id': con[33],
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

            if con.get('analytic_account_id'):
                account_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'account.analytic.account', 'search', [[['old_id', '=', con.get('analytic_account_id')], *archieved_condition]])
                if account_id:
                    con.update({'analytic_account_id': account_id[0]})
            
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.contract', 'create', [con])
            print(f'{index+1}/{len(db_contracts)}')



def insert_2():
    cr.execute("""
        SELECT
            tor.contract_id,
            array_agg(tor.desc)
        FROM
            termsofreference tor
        WHERE
            tor.contract_id IS NOT NULL
        GROUP BY
            tor.contract_id
    """)
    tors = cr.fetchall()

    for index, tor in enumerate(tors):
        contract_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.contract', 'search', [[['old_id', '=', tor[0]], *archieved_condition]])
        if contract_id:
            terms = [{'name': term, 'contract_id': contract_id[0]} for term in tor[1]]
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'contract.general.terms', 'create', [terms])
            print(f'{index+1}/{len(tors)}')


def insert_3():
    cr.execute("""
        SELECT
            id,
            name,
            active,
            is_company,
            company_id
        FROM
            res_partner
        WHERE
            id IN (SELECT DISTINCT(service_provider_id) FROM hr_service_contract)
        ORDER BY
            id
    """)

    partners = [{
        'old_id': partner[0],
        'name': partner[1],
        'active': partner[2],
        'is_company': partner[3],
        'company_id': partner[4]

    } for partner in cr.fetchall()]

    odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'create', [partners])




def insert_4():
    cr.execute("""
        SELECT
            service.id,
            service.name,
            service.department_id,
            service.date_start::TEXT,
            service.date_end::TEXT,
            service.salary_currency,
            tm.name as service_name,
            CASE 
                WHEN service.salary_currency = 'USD' 
                    THEN service.wage 
                ELSE 
                    service.wage_afn
            END AS wage,
            service.company_id,
            service.state,
            service.tor_text,
            service.service_provider_id
        FROM
            hr_service_contract service LEFT JOIN res_partner partner ON partner.id = service.service_provider_id
            LEFT JOIN service_template tm ON tm.id = service.service_id
        GROUP BY
            service.id,
            service.name,
            tm.name
        ORDER BY 
            service.id
    """)

    db_service_contracts = [{
        'old_id': serv[0],
        'agreement_reference': serv[1],
        'department_id': serv[2],
        'start_date': serv[3],
        'end_date': serv[4],
        'currency_id': serv[5],
        'name': serv[6] or 'Untitled',
        'amount': serv[7],
        'company_id': serv[8],
        'state': serv[9],
        'terms_of_reference': serv[10],
        'service_provider_id': serv[11]
    } for serv in cr.fetchall()]

    currency_ids = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.currency', 'search_read', [[['name', 'in', ['USD', 'AFN']], *archieved_condition]])

    currencies = {f"{currency_ids[0].get('name')}": currency_ids[0].get('id'), f"{currency_ids[1].get('name')}": currency_ids[1].get('id')}

    states = {
        'draft': '1_draft',
        'manager_approval': '2_under_review',
        'controller_approval': '3_finance_review',
        'ceo_approval': '4_director_approval',
        'open': '5_running',
        'close': '9_expired',
        'cancel': '7_cancelled',
        'rejected': '8_rejected'
    }

    for index, serv in enumerate(db_service_contracts):
        if serv.get('department_id'):
            dep_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', serv.get('department_id')], *archieved_condition]])
            serv.update({'department_id': dep_id and dep_id[0] or False})
        if serv.get('service_provider_id'):
            service_provider_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'search', [[['old_id', '=', serv.get('partner_id')], *archieved_condition]])    
            serv.update({'service_provider_id': service_provider_id and service_provider_id[0] or False})
        serv.update({'state': states.get(serv.get('state')), 'currency_id': currencies.get(serv.get('currency_id', '0'), False)})

        odoo.execute_kw(O_DB, O_UID, O_PWD, 'service.contract', 'create', [serv])
        print(f'{index+1}/{len(db_service_contracts)}')



# insert_1()
# insert_2()
# insert_3()
# insert_4()
