CREATE TABLE test.sales (

	sale_ID Integer UNIQUE NOT NULL,
	sale_user_ID Integer ,
	sale_timestamp timestamp,
	sale_type text
);
 
CREATE TABLE test.payments (

	pay_sale_ID 	Integer UNIQUE NOT NULL,
	pay_timestamp 	timestamp,
	pay_amount    	numeric
	);


CREATE TABLE test.users (

	user_id				INTEGER,
	user_gender			TEXT,
	user_city			TEXT,
	user_registration_timestamp	timestamp

	);


CREATE TABLE test.mkt_contact (

	contact_user_id 	Integer UNIQUE NOT NULL,
	contact_channel_id 	TEXT,
	contact_timestamp 	timestamp,
	contact_is_last		boolean
	);

CREATE TABLE test.mkt_channel (
	channel_id text,
	channel_name text
	);

CREATE TABLE test.mkt_info (
	info_channel_id    	text, 
	info_timestamp  	timestamp,
	info_clicks		Integer,
	info_impressions	integer
	);

--1a

EXPLAIN ANALYZE
SELECT 	users.user_city
	,sum( pms.pay_amount ) 			as total_payment_amount
FROM	test.users	users
JOIN	test.sales	sales
ON	users.user_id = sales.sale_user_id
JOIN   (
	SELECT 	pay_amount
		,pay_sale_id
	FROM	test.payments
	WHERE   date_part('year',pay_timestamp) =  '2017'
       ) pms
ON 	pms.pay_sale_id = sales.sale_ID	
GROUP BY users.user_city
 

--1b
SELECT 	    count(users.user_id)
FROM	    test.users   users
JOIN        (
		SELECT 	   mkt_ord.contact_user_id
			   ,count(distinct mkt_ord.contact_channel_id) as channel_count
		FROM  	   test.mkt_contact mkt_ord
		GROUP BY   mkt_ord.contact_user_id
		HAVING     count(distinct mkt_ord.contact_channel_id) >= 2
		) cond1
ON	    users.user_id = cond1.contact_user_id
JOIN        (
		SELECT mkt_contact_order.contact_user_id
		FROM (
				
			SELECT 	    mkt_contact.contact_user_id
				   ,mkt_contact.contact_channel_id
				   ,mkt_contact.contact_timestamp
				   ,mkt_channel.channel_name
				   ,rank() OVER  (PARTITION BY mkt_contact.contact_user_id ORDER BY mkt_contact.contact_timestamp DESC) as mkt_channel_rank
			FROM  	   test.mkt_contact mkt_contact
			LEFT  JOIN test.mkt_channel mkt_channel
			ON	   mkt_contact.contact_channel_id = mkt_channel.channel_id
			) mkt_contact_order
		WHERE  mkt_contact_order.mkt_channel_rank = 1
		AND    mkt_contact_order.channel_name = 'SEM'
		) cond2
ON	    users.user_id = cond2.contact_user_id

 
--1c

SELECT 	  mkt_per_user.contact_channel_id		    as contact_channel
	  ,mkt_per_user.user_registration_date	            as registration_date
	  ,count(mkt_per_user.contact_user_id) 		    as total_users
FROM(
	SELECT 	   mkt_contact.contact_user_id
	           ,mkt_contact.contact_channel_id 
	           ,users.user_registration_timestamp::date as user_registration_date
	FROM  	   test.mkt_contact 		mkt_contact
	JOIN       test.users			users
	ON	   mkt_contact.contact_user_id = users.user_id
	) mkt_per_user
GROUP   BY mkt_per_user.contact_channel_id
	   ,mkt_per_user.user_registration_date


--1d

SELECT  date_dim.rec_date 
	,coalesce(click.count_clik,0)  as count_clik
	,coalesce(register.count_register,0)  as count_register
FROM (

	SELECT  distinct  info_timestamp::date 	  		  as rec_date 
	FROM      test.mkt_info
	UNION
	SELECT  distinct  user_registration_timestamp::date 	  as rec_date 
	FROM      test.users
	) date_dim	
LEFT JOIN 	(
			SELECT    info_timestamp::date 	  as rec_date
				  ,count(info_channel_id) as count_clik	
			FROM      test.mkt_info
			GROUP BY  info_timestamp::date
		)  click	
ON    date_dim.rec_date =  click.rec_date
LEFT JOIN 	(
			SELECT    user_registration_timestamp::date  as rec_date
				  ,count(user_id)	             as count_register
		        FROM	  test.users
		        GROUP BY  user_registration_timestamp::date 
		)  register
ON    date_dim.rec_date =  register.rec_date
WHERE date_part('year',date_dim.rec_date) =  '2017'

 





--3a
SELECT sales_trans.user_registration_timestamp::date    as registration_date
	,sales_trans.contact_channel_id  		as last_channel_id
	,count(distinct sales_trans.user_id) 		as total_users
	,count(sales_trans.sale_id) 			as total_sales
FROM (
        -- granularity : sales / registration date
		SELECT users.user_registration_timestamp
		       ,users.user_id
		       ,mkt_last_contact.contact_channel_id
		       ,sales.sale_timestamp
		       ,sales.sale_id
		       ,extract(DAY FROM (sales.sale_timestamp - users.user_registration_timestamp)) as sales_lead_time
		FROM   test.users  users
		JOIN (
		-- last contact
			SELECT mkt_contact.contact_channel_id
				,mkt_contact.contact_user_id
			FROM   test.mkt_contact mkt_contact
			WHERE  mkt_contact.contact_is_last=TRUE
			) mkt_last_contact
		ON users.user_id=mkt_last_contact.contact_user_id
		JOIN   test.sales	sales
		ON  	users.user_id=sales.sale_user_id 
	) sales_trans
WHERE    sales_trans.sales_lead_time <= 3
GROUP BY sales_trans.user_registration_timestamp::date
	,sales_trans.contact_channel_id 

--3b

SELECT  click.rec_date 
	,coalesce(click.count_clik,0)  as 	 count_clik
	,coalesce(register.count_user_id,0)  as count_register
FROM 
	(
			SELECT    info_timestamp::date 	  as rec_date
				  ,count(info_channel_id) as count_clik	
			FROM      test.mkt_info
			GROUP BY  info_timestamp::date
		)  click	
LEFT JOIN 
(       -- user whom redirected from market online channel and then do registration.
	SELECT  user_registration_date			 as rec_date
		,count(user_id)				 as count_user_id
	FROM (
	SELECT  mkt_contact.contact_timestamp::date	 as contact_date
		,mkt_contact.contact_user_ID
		,users.user_id
		,users.user_registration_timestamp::date as user_registration_date
	FROM	test.mkt_contact   mkt_contact
	JOIN	test.users         users
	ON	users.user_id = mkt_contact.contact_user_ID
	JOIN	test.mkt_info      mkt_info
	ON	mkt_contact.contact_timestamp = mkt_info.info_timestamp
	AND     mkt_contact.contact_channel_id = mkt_info.info_channel_id
	      ) user_register
	GROUP BY user_registration_date
)  register
ON    register.rec_date =  click.rec_date

