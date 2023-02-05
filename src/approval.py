from config.db import init_connection


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()

archieved_condition = ['|', ['active', '=', True], ['active', '=', False]]


def approval():
    cr.execute("""
        SELECT
            id,
            name,
            request_owner_id,
            category_id,
            date_confirmed::TEXT,
            date::TEXT,
            location,
            amount,
            currency_id,
            urgent_level,
            approval_seq,
            reason,
            request_status
        FROM
            approval_request
        WHERE
            request_status = 'approved'
        ORDER BY
            id
    """)
    index = 0
    for app in cr.fetchall():
        vals = {
            'old_id': app[0],
            'name': app[1],
            'request_owner_id': app[2],
            'category_id': 3,
            'date_confirmed': app[4],
            'date': app[5],
            'location': app[6],
            'amount': app[7],
            # 'currency_id': app[8],
            # 'urgent_level': app[9],
            'old_sequence': app[10],
            'reason': app[11],
            'request_status': app[12],
            'old_total': app[7]
        }
        try:
            request_owner_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.users', 'search', [[['old_id', '=', app[2]], *archieved_condition]])
            vals.update({'request_owner_id': request_owner_id and request_owner_id[0] or False})
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.request', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e)

def approval_lines():
    cr.execute("""
        SELECT
            item.approval_id AS approval_id,
            t.name As name,
            item.quantity AS quantity,
            item.unit_price AS unit_price,
            uom.name AS uom,
            bg.name AS donor_code,
            acc.name AS budget_code,
            item.item_availability AS availability,
            item.model AS model

        FROM
            approval_items AS item
        INNER JOIN approval_request app ON app.id = item.approval_id
        LEFT JOIN product_product AS pd ON pd.id = item.name1
        LEFT JOIN crossovered_budget_lines AS budget ON budget.id = item.budget_line_id
        LEFT JOIN uom_uom AS uom ON uom.id = item.unit_of_measure
        LEFT JOIN crossovered_budget AS bg ON bg.id = item.budget_id
        LEFT JOIN product_template AS t ON t.id = pd.product_tmpl_id
        LEFT JOIN account_analytic_account AS acc ON acc.id = budget.analytic_account_id
        WHERE
            request_status = 'approved'
        ORDER BY
            item.approval_id
    """)
    index = 0
    for item in cr.fetchall():
        vals = {
            'approval_id': item[0],
            'name': item[1],
            'quantity': float(f'{item[2]}') if item[2] else 0,
            'unit_price': float(f'{item[3]}') if item[3] else 0,
            'uom': item[4],
            'donor_code': item[5],
            'budget_code': item[6],
            'availability': item[7],
            'model': item[8]
        }
        vals.update({'subtotal': vals.get('quantity') * vals.get('unit_price')})

        try:
            approval_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.request', 'search', [[['old_id', '=', item[0]]]])
            vals.update({'approval_id': approval_id and approval_id[0] or False})
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.oldlines', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e, item[0], item[1])

def approval_approvers():
    cr.execute("""
        SELECT
            apr.request_id AS request_id,
            apr.user_id AS user_id,
            apr.status AS status
        FROM
            approval_approver AS apr
        INNER JOIN
            approval_request app ON app.id = apr.request_id
        WHERE
            app.request_status = 'approved'
        ORDER BY
            apr.id
    """)
    index = 0
    for item in cr.fetchall():
        vals = {
            'request_id': item[0],
            'user_id': item[1],
            'status': item[2]
        }

        try:
            request_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.request', 'search', [[['old_id', '=', item[0]]]])
            vals.update({'request_id': request_id and request_id[0] or False})

            user_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.users', 'search', [[['old_id', '=', item[1]], *archieved_condition]])
            vals.update({'user_id': user_id and user_id[0] or False})

            odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.approver', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e, item[0], item[1])




def purchase_requistion():
    cr.execute("""
        SELECT
            id,
            name,
            exclusive,
            quantity_copy,
            line_copy,
            link_to_service_contract,
            is_blanket_order
        FROM
            purchase_requisition_type
        WHERE
            id > 2
        ORDER BY
            id
    """)

    for index, pur in enumerate(cr.fetchall()):
        vals = {
            'old_id': pur[0],
            'name': pur[1],
            'exclusive': pur[2],
            'quantity_copy': pur[3],
            'line_copy': pur[4],
            # 'link_to_service_contract': pur[5],
            # 'is_blanket_order': pur[6] 
        }
        try:
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.requisition.type', 'create', [vals])
            print(index+1)
            index+=1
        except Exception as e:
            print(f"Error: {pur[0]}-{pur[1]}",e)


    cr.execute("""
        SELECT
            id,
            name,
            origin,
            vendor_id,
            type_id,
            ordering_date::TEXT,
            date_end::TEXT,
            schedule_date::TEXT,
            user_id,
            description,
            company_id,
            state,
            currency_id,
            title,
            winner_id,
            finance_approval_date::TEXT,
            director_approval_date::TEXT,
            warehouse_id,
            picking_type_id,
            approval_id,
            show_service_contract,
            source_approval_requests
        FROM
            purchase_requisition
        ORDER BY
            id
    """)

    for index, pq in enumerate(cr.fetchall()):
        vals = {
            'old_id': pq[0],
            'name': pq[1],
            'origin': pq[2],
            'vendor_id': pq[3],
            'type_id': pq[4],
            'ordering_date': pq[5],
            'date_end': pq[6],
            'schedule_date': pq[7],
            'user_id': pq[8],
            'description': pq[9],
            'company_id': pq[10],
            'state': pq[11],
            'currency_id': pq[12],
            'title': pq[13] or 'N/A',
            'winner_id': False,
            'finance_approval_date': pq[15],
            'director_approval_date': pq[16],
            'warehouse_id': pq[17],
            'picking_type_id': pq[18],
            # 'approval_id': pq[19],
            # 'show_service_contract': pq[20],
            # 'source_approval_requests': pq[21]
        }
        try:
            vendor_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'search', [[['old_id', '=', pq[3]], *archieved_condition]])
            vals.update({'vendor_id': vendor_id and vendor_id[0] or False})

            # approval_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.request', 'search', [[['old_id', '=', pq[19]]]])
            # vals.update({'approval_id': approval_id and approval_id[0] or False})

            odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.requisition', 'create', [vals])
            print(index+1)
            index+=1
        except Exception as e:
            print(f"Error: {pq[0]}-{pq[1]}",e)
    

def purchase_requistion_lines():
    cr.execute("""
        SELECT
            pt.name as name,
            line.product_qty,
            line.price_unit,
            uom.name as uom,
            line.schedule_date::TEXT,
            acc.name AS account,
            line.requisition_id
        FROM
            purchase_requisition_line AS line
        LEFT JOIN
            product_product AS pd ON pd.id = line.product_id
        LEFT JOIN
            product_template AS pt ON pt.id = pd.product_tmpl_id
        LEFT JOIN
            uom_uom AS uom ON uom.id = line.product_uom_id
        LEFT JOIN
            account_analytic_account AS acc ON acc.id = line.account_analytic_id
        ORDER BY
            line.id
    """)
    index = 0
    for item in cr.fetchall():
        vals = {
            'name': item[0],
            'quantity': float(f'{item[1]}') if item[1] else 0,
            'unit_price': float(f'{item[2]}') if item[2] else 0,
            'uom': item[3],
            'scheduled_date': item[4],
            'analytic_account': item[5],
            'purchase_requisition_id': item[6]
        }

        try:
            purchase_requisition_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.requisition', 'search', [[['old_id', '=', item[6]]]])
            vals.update({'purchase_requisition_id': purchase_requisition_id and purchase_requisition_id[0] or False})
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.requisition.oldlines', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e, item[0], item[6])



def purchase_approvers():
    cr.execute("""
        SELECT
            apr.request_id AS request_id,
            apr.user_id AS user_id,
            apr.status AS status,
            apr.approve_date::TEXT
        FROM
            purchase_approver AS apr
        INNER JOIN
            purchase_requisition req ON req.id = apr.request_id
        ORDER BY
            apr.id
    """)
    index = 0
    for item in cr.fetchall():
        vals = {
            'requisition_id': item[0],
            'user_id': item[1],
            'state': item[2] if item[2] != 'cancel' else 'refused',
            'date': item[3]
        }

        try:
            requisition_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.requisition', 'search', [[['old_id', '=', item[0]]]])
            vals.update({'requisition_id': requisition_id and requisition_id[0] or False})

            user_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.users', 'search', [[['old_id', '=', item[1]], *archieved_condition]])
            vals.update({'user_id': user_id and user_id[0] or False})

            odoo.execute_kw(O_DB, O_UID, O_PWD, 'panel.member', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e, item[0], item[1])


def purchase_order():
    cr.execute("""
        SELECT
            id,
            name,
            origin,
            partner_ref,
            date_order::TEXT,
            date_approve::TEXT,
            partner_id,
            currency_id,
            state,
            notes,
            invoice_status,
            date_planned::TEXT,
            amount_untaxed,
            amount_tax,
            amount_total,
            payment_term_id,
            user_id,
            currency_rate,
            picking_type_id,
            approval_id,
            requisition_id,
            rfq_due_date::TEXT,
            source_approval_requests,
            winner,
            equipment_created
        FROM
            purchase_order
        """)
    index = 0
    for order in cr.fetchall():
        vals = {
            'old_id': order[0],
            'name': order[1],
            'origin': order[2],
            'partner_ref': order[3],
            'date_order': order[4],
            'date_approve': order[5],
            'partner_id': order[6],
            'currency_id': order[7],
            'state': order[8] if order[8] != 'ceo' else 'director_approval',
            'notes': order[9],
            'invoice_status': order[10],
            'date_planned': order[11],
            'amount_untaxed': float(f'{order[12]}') if order[12] else 0,
            'amount_tax': float(f'{order[13]}') if order[13] else 0,
            'amount_total': float(f'{order[14]}') if order[14] else 0,
            'payment_term_id': order[15],
            'user_id': order[16],
            'currency_rate': float(f'{order[17]}') if order[17] else 0,
            'picking_type_id': order[18],
            # 'approval_id': order[19],
            'requisition_id': order[20],
            # 'rfq_due_date': order[21],
            # 'source_approval_requests': order[22],
            'winner': order[23],
            'has_equipment': order[24]
        }

        vals.update({
            'old_amount_tax': vals.get('amount_tax'),
            'old_amount_untaxed': vals.get('amount_untaxed'),
            'old_amount_total': vals.get('amount_total')
        })

        try:
            requisition_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.requisition', 'search', [[['old_id', '=', order[20]]]])
            vals.update({'requisition_id': requisition_id and requisition_id[0] or False})

            user_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.users', 'search', [[['old_id', '=', order[16]], *archieved_condition]])
            vals.update({'user_id': user_id and user_id[0] or False})

            partner_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'search', [[['old_id', '=', order[6]], *archieved_condition]])
            vals.update({'partner_id': partner_id and partner_id[0] or False})

            # approval_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'approval.request', 'search', [[['old_id', '=', order[19]]]])
            # vals.update({'approval_id': approval_id and approval_id[0] or False})


            odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.order', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e, order[0], order[1])


def purchase_order_lines():
    cr.execute("""
        SELECT
            pt.name as name,
            line.name as description,
            bg.name as donor_code,
            line.product_qty,
            uom.name as uom,
            line.price_unit,
            tax.name as tax_names,
            tax.amount as tax_amount,
            line.order_id
        FROM
            purchase_order_line AS line
        LEFT JOIN
            product_product AS pd ON pd.id = line.product_id
        LEFT JOIN
            product_template AS pt ON pt.id = pd.product_tmpl_id
        LEFT JOIN
            uom_uom AS uom ON uom.id = line.product_uom
        LEFT JOIN
            account_tax_purchase_order_line_rel AS tax_rel ON tax_rel.purchase_order_line_id = line.id
        LEFT JOIN
            account_tax tax ON tax.id = tax_rel.account_tax_id
        LEFT JOIN
            crossovered_budget bg ON bg.id = line.crossovered_budget_id
        ORDER BY
            line.id
    """)
    index = 0
    for item in cr.fetchall():
        vals = {
            'name': item[0],
            'description': item[1],
            'donor_code': item[2],
            'quantity': float(f'{item[3]}') if item[3] else 0,
            'uom': item[4],
            'unit_price': float(f'{item[5]}') if item[5] else 0,
            'taxes': item[6],
            'purchase_order_id': item[8]
        }
        vals.update({'subtotal': vals.get('unit_price') * vals.get('quantity')})
        if item[6]:
            vals.update({'subtotal': (vals.get('subtotal') / 100) * (100 - float(f'{item[7]}'))})
        try:
            purchase_order_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.order', 'search', [[['old_id', '=', item[8]]]])
            vals.update({'purchase_order_id': purchase_order_id and purchase_order_id[0] or False})
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'purchase.order.oldlines', 'create', [vals])
            index+=1
            print(index)

        except Exception as e:
            print('ERROR', e, item[0], item[8])

# Run each one separately (All others should be commented each time)

# approval()
# approval_lines()
# approval_approvers()

# purchase_requistion()
# purchase_requistion_lines()
# purchase_approvers()

# purchase_order()
# purchase_order_lines()