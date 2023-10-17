select distinct il.packagetypeid, i.invoicedate
from {{ ref('datastore_invoicelines') }} as il
left join {{ ref('datastore_invoices') }} as i on i.invoiceid = il.invoiceid
order by il.packagetypeid, i.invoicedate
-- where invoiceid = 5
