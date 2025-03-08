with
  pagamentos as (
    select
      pgtos.*,
      itens.seller_id
    from
      data_warehouse.orders as pedidos
      left join data_warehouse.order_payments as pgtos
      on pedidos.order_id = pgtos.order_id
      left join data_warehouse.order_items as itens
      on pedidos.order_id = itens.order_id
    where
      date(pedidos.order_purchase_timestamp) < date('2018-01-01')
      and date(pedidos.order_purchase_timestamp) >= DATE_ADD(date('2018-01-01'), INTERVAL -6 MONTH)
      and itens.seller_id is not null
  ),

  pedidos as (
    select
      seller_id,
      payment_type,
      count(distinct order_id) as qt_pedidos_meio,
      sum(payment_value) as vl_pedidos_meio
    from
      pagamentos
    group by
      seller_id,
      payment_type
    order by
      seller_id,
      payment_type
  )

select
  seller_id,

  sum(case when payment_type = 'credit_card' then qt_pedidos_meio else 0 end) as qt_pedidos_credito,
  sum(case when payment_type = 'debit_card' then qt_pedidos_meio else 0 end) as qt_pedidos_debito,
  sum(case when payment_type = 'boleto' then qt_pedidos_meio else 0 end) as qt_pedidos_boleto,
  sum(case when payment_type = 'voucher' then qt_pedidos_meio else 0 end) as qt_pedidos_voucher,

  sum(case when payment_type = 'credit_card' then vl_pedidos_meio else 0 end) as vl_pedidos_credito,
  sum(case when payment_type = 'debit_card' then vl_pedidos_meio else 0 end) as vl_pedidos_debito,
  sum(case when payment_type = 'boleto' then vl_pedidos_meio else 0 end) as vl_pedidos_boleto,
  sum(case when payment_type = 'voucher' then vl_pedidos_meio else 0 end) as vl_pedidos_voucher,
    
  sum(case when payment_type = 'credit_card' then qt_pedidos_meio else 0 end) / sum(qt_pedidos_meio) as pct_qt_pedidos_credito,
  sum(case when payment_type = 'debit_card' then qt_pedidos_meio else 0 end) / sum(qt_pedidos_meio) as pct_qt_pedidos_debito,
  sum(case when payment_type = 'boleto' then qt_pedidos_meio else 0 end) / sum(qt_pedidos_meio) as pct_qt_pedidos_boleto,
  sum(case when payment_type = 'voucher' then qt_pedidos_meio else 0 end) / sum(qt_pedidos_meio) as pct_qt_pedidos_voucher,

  sum(case when payment_type = 'credit_card' then vl_pedidos_meio else 0 end) / sum(vl_pedidos_meio) as pct_vl_pedidos_credito,
  sum(case when payment_type = 'debit_card' then vl_pedidos_meio else 0 end) / sum(vl_pedidos_meio) as pct_vl_pedidos_debito,
  sum(case when payment_type = 'boleto' then vl_pedidos_meio else 0 end) / sum(vl_pedidos_meio) as pct_vl_pedidos_boleto,
  sum(case when payment_type = 'voucher' then vl_pedidos_meio else 0 end) / sum(vl_pedidos_meio) as pct_vl_pedidos_voucher

from
  pedidos
group by
  seller_id