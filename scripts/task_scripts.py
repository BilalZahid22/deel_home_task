task_1= """
select count(contract_id) as total, received_at::date, is_deleted
from contracts
where is_current is true
group by received_at::date, is_deleted
order by 2 desc
;
"""


task_2 = """
select contract_id, received_at::date, is_deleted, count(invoice_id) as total
from invoices
where is_current is true
group by contract_id, received_at::date, is_deleted
order by 1, 2, 4 desc
;
"""

task_3 = """
select contract_id, received_at::date, currency, SUM(amount) total
from invoices
where is_deleted = false
group by contract_id, received_at::date, currency
order by 1, 2
;

"""


task_4 = """select *
from (select invoice_id,
             A.contract_id,
             A.received_at as                                                                 invoice_received_at,
             B.received_at as                                                                 contract_received_at,
             client_id
              ,
             row_number() over (PARTITION BY invoice_id order by A.received_at,B.received_at) dup_row
      from invoices A
               join contracts B on A.contract_id = B.contract_id
      where A.is_deleted = false
        and B.is_deleted is false
      order by 1) A
where dup_row = 1
"""

