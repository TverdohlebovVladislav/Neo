create or replace procedure ds.fill_account_turnover_f (
   i_OnDate date
)
language plpgsql    
as $$
declare
	v_RowCount int;
begin
	
	call logs.writelog( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    call logs.writelog( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1
       );
	   
    delete
      from dm.dm_account_turnover_f f
     where f.on_date = i_OnDate;
   
    call logs.writelog('insert', 1);
	
    insert
      into dm.dm_account_turnover_f
           ( on_date
           , account_rk
           , credit_amount
           , credit_amount_rub
           , debet_amount
           , debet_amount_rub
           )
    with wt_turn as
    ( select p.credit_account_rk                  as account_rk
           , p.credit_amount                      as credit_amount
           , p.credit_amount * nullif(er.reduced_cource, 1)         as credit_amount_rub
           , cast(null as numeric)                 as debet_amount
           , cast(null as numeric)                 as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.credit_account_rk
        left
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date   and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date    and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
       union all
      select p.debet_account_rk                   as account_rk
           , cast(null as numeric)                 as credit_amount
           , cast(null as numeric)                 as credit_amount_rub
           , p.debet_amount                       as debet_amount
           , p.debet_amount * nullif(er.reduced_cource, 1)          as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.debet_account_rk
        left 
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
    )
    select i_OnDate                               as on_date
         , t.account_rk
         , sum(t.credit_amount)                   as credit_amount
         , sum(t.credit_amount_rub)               as credit_amount_rub
         , sum(t.debet_amount)                    as debet_amount
         , sum(t.debet_amount_rub)                as debet_amount_rub
      from wt_turn t
     group by t.account_rk;
	 
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call logs.writelog('[END] inserted ' || to_char(v_RowCount,'FM99999999') || ' rows.', 1);

    commit;
	
end;$$