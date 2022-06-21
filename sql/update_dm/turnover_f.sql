update dm.dm_account_turnover_f
set credit_amount = 0
where credit_amount is null;

update dm.dm_account_turnover_f
set credit_amount_rub = 0
where credit_amount_rub is null;

update dm.dm_account_turnover_f
set debet_amount = 0
where debet_amount is null;

update dm.dm_account_turnover_f
set debet_amount_rub = 0
where debet_amount_rub is null;