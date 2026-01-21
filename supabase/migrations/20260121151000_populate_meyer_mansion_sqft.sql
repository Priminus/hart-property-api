-- Populate sqft for Meyer Mansion transactions
-- Matching on purchase_price and purchase_date to identify unique transactions

-- 3BR, 1109 sqft - purchase 26 Apr 2022 $2,788,000
update public.condo_sale_transactions
set sqft = 1109
where condo_name = 'Meyer Mansion'
  and purchase_price = 2788000
  and purchase_date = '2022-04-26';

-- 3BR, 1109 sqft - purchase 25 Jul 2022 $2,840,800
update public.condo_sale_transactions
set sqft = 1109
where condo_name = 'Meyer Mansion'
  and purchase_price = 2840800
  and purchase_date = '2022-07-25';

-- 2BR, 689 sqft - purchase 31 May 2022 $1,888,000
update public.condo_sale_transactions
set sqft = 689
where condo_name = 'Meyer Mansion'
  and purchase_price = 1888000
  and purchase_date = '2022-05-31';

-- 3BR, 1109 sqft - purchase 19 Dec 2021 $2,925,000
update public.condo_sale_transactions
set sqft = 1109
where condo_name = 'Meyer Mansion'
  and purchase_price = 2925000
  and purchase_date = '2021-12-19';

-- 4BR, 1722 sqft - purchase 02 Oct 2020 $4,635,800
update public.condo_sale_transactions
set sqft = 1722
where condo_name = 'Meyer Mansion'
  and purchase_price = 4635800
  and purchase_date = '2020-10-02';

-- 4BR, 1722 sqft - purchase 07 Mar 2022 $4,527,000
update public.condo_sale_transactions
set sqft = 1722
where condo_name = 'Meyer Mansion'
  and purchase_price = 4527000
  and purchase_date = '2022-03-07';

-- 3BR, 1496 sqft - purchase 13 Sep 2019 $3,810,000
update public.condo_sale_transactions
set sqft = 1496
where condo_name = 'Meyer Mansion'
  and purchase_price = 3810000
  and purchase_date = '2019-09-13';

-- 3BR, 1109 sqft - purchase 02 Aug 2021 $2,866,900
update public.condo_sale_transactions
set sqft = 1109
where condo_name = 'Meyer Mansion'
  and purchase_price = 2866900
  and purchase_date = '2021-08-02';

-- 4BR, 1765 sqft - purchase 29 Mar 2021 $4,638,100
update public.condo_sale_transactions
set sqft = 1765
where condo_name = 'Meyer Mansion'
  and purchase_price = 4638100
  and purchase_date = '2021-03-29';

-- 3BR, 1109 sqft - purchase 01 Jan 2021 $3,046,500
update public.condo_sale_transactions
set sqft = 1109
where condo_name = 'Meyer Mansion'
  and purchase_price = 3046500
  and purchase_date = '2021-01-01';

-- 4BR, 1765 sqft - purchase 25 Sep 2019 $4,600,300
update public.condo_sale_transactions
set sqft = 1765
where condo_name = 'Meyer Mansion'
  and purchase_price = 4600300
  and purchase_date = '2019-09-25';
