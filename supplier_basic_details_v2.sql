WITH cte1 as (
  SELECT 
    suppliers.id AS dim__supplier_id, 
    suppliers.gstin AS supplier_gstin, 
    main2.gst_type AS supplier_gst_type, 
    suppliers.name AS supplier_name, 
    CASE WHEN new_valid.valid IS TRUE THEN 1 ELSE 0 END AS supplier_valid, 
    pincode_city_tier.city AS supplier_city, 
    pincode_city_tier.state_code AS supplier_state, 
    CASE WHEN pincode_city_tier.state_code IN (
      'WB', 'OR', 'MZ', 'MN', 'NL', 'AS', 'AR', 
      'ML', 'SK', 'TR', 'AR'
    ) THEN 'East' WHEN pincode_city_tier.state_code IN (
      'UP', 'BR', 'CG', 'HR', 'UK', 'HP', 'CH', 
      'DL', 'JH', 'MP', 'PB', 'JK'
    ) THEN 'North' WHEN pincode_city_tier.state_code IN ('TN', 'TS', 'KL', 'PU', 'AP', 'KA') THEN 'South' WHEN pincode_city_tier.state_code IN ('GJ', 'MH', 'DD', 'GA', 'RJ') THEN 'West' END AS region, 
    date(suppliers.created) AS supplier_onboarding_date, 
    suppliers.qc_status_id AS supplier_qc_status, 
    CAST(
      TO_TIMESTAMP(
        SUBSTRING(
          CAST(deactivation_date AS STRING), 
          1, 
          10
        ), 
        'yyyy-MM-dd'
      ) AS DATE
    ) AS deactivation_date, 
    suppliers.deactivation_reason as deactivation_Reason, 
    suppliers.phone AS supplier_phone, 
    suppliers.email AS supplier_email, 
    supplier_bank.account_number AS supplier_bank_account_number, 
    a.d1 AS first_qc_status_change_date, 
    b.d1 AS first_aq_movement_date, 
    suppliers.address_line AS supplier_address_line, 
    suppliers.pin AS supplier_pincode, 
    bd_executives.name AS BD_name, 
    bd_executives.email AS BD_Email, 
    BD_mapping.bd_lead AS BD_lead, 
    suppliers.supplier_type AS supplier_type, 
    suppliers.deactivated_by as deactivation_by, 
    suppliers.international AS is_supplier_international, 
    suppliers.exclusive AS is_supplier_exclusive, 
    suppliers.identifier AS supplier_identifier, 
    CASE WHEN main1.id IS NOT NULL THEN 1 ELSE 0 END AS meesho_trusted_supplier, 
    main5.likely_to as last_30_days_nps_score, 
    main6.source as supplier_acquisition_source, 
    cast(main6.referral_flag as int) as referral_flag 
  FROM 
    (
      select 
        * 
      from 
        silver.supply__suppliers 
      where 
        year >= 2015
    ) suppliers 
    LEFT JOIN silver.supply__bd_executives bd_executives ON bd_executives.id = suppliers.bdexec_id 
    LEFT JOIN (
      select 
        * 
      from 
        silver.supply__supply_supplier_bank_details 
      where 
        year >= 2015
    ) supplier_bank ON supplier_bank.supplier_id = suppliers.id 
    LEFT JOIN mercury.gdrive__bd_mapping BD_mapping ON bd_executives.name = BD_mapping.BD_name 
    LEFT JOIN mercury.pincode_city_tier_updated pincode_city_tier ON CAST(
      pincode_city_tier.pin_code AS STRING
    ) = suppliers.pin 
    LEFT JOIN (
      SELECT 
        data_id AS supplier_id, 
        MIN(timestamp) AS d1 
      FROM 
        silver.supply__status_change_logs status_change_logs 
      WHERE 
        type = 'supplier_qc_status' 
        and year >= 2015 
      GROUP BY 
        1
    ) AS a ON a.supplier_id = suppliers.id 
    LEFT JOIN (
      SELECT 
        data_id AS supplier_id, 
        old_status, 
        new_status, 
        timestamp AS t0, 
        MIN(timestamp) OVER (PARTITION BY data_id) AS d1 
      FROM 
        silver.supply__status_change_logs status_change_logs 
      WHERE 
        type = 'supplier_qc_status' 
        AND old_status IN (1, 7, 11) 
        AND new_status IN (2, 3) 
        and year >= 2015 
      GROUP BY 
        1, 
        2, 
        3, 
        4
    ) AS b ON b.supplier_id = suppliers.id 
    LEFT JOIN (
      SELECT 
        id 
      FROM 
        silver.supplier_detail__supplier_detail_supplier supplier_detail_supplier 
      WHERE 
        trusted = TRUE 
        and year >= 2015 
      GROUP BY 
        1
    ) AS main1 ON suppliers.id = main1.id 
    LEFT JOIN (
      SELECT 
        a.supplier_id, 
        b.gst_type 
      FROM 
        (
          SELECT 
            DISTINCT id AS supplier_id, 
            supplier_kyc_id 
          FROM 
            silver.supplier_detail__supplier_detail_supplier 
          WHERE 
            year >= 2015
        ) a 
        INNER JOIN (
          SELECT 
            DISTINCT id AS supplier_kyc_id, 
            gst_type 
          FROM 
            silver.supplier_detail__supplier_detail_supplier_kyc 
          WHERE 
            gst_type IS NOT NULL 
            and year >= 2015
        ) b ON a.supplier_kyc_id = b.supplier_kyc_id
    ) AS main2 ON suppliers.id = main2.supplier_id 
    LEFT JOIN (
      SELECT 
        id, 
        valid 
      FROM 
        silver.supplier_detail__supplier_detail_supplier 
      WHERE 
        year >= 2015 
      GROUP BY 
        1, 
        2
    ) AS new_valid ON suppliers.id = new_valid.id 
    left join (
      select 
        supplier_id, 
        likely_to 
      from 
        (
          select 
            supplier_id, 
            submitted_at, 
            date(submitted_at) as date_of_record, 
            likely_to, 
            row_number() over (
              partition by supplier_id 
              order by 
                submitted_at desc
            ) as rank1 
          from 
            silver.gdrive__daily_supplier_nps_tag daily_supplier_nps_tag 
          where 
            submitted_at >= current_date - interval '30' day 
          group by 
            1, 
            2, 
            3, 
            4
        ) 
      where 
        rank1 = 1 
      group by 
        1, 
        2
    ) main5 on cast(main5.supplier_id as string)= cast(suppliers.id as string) 
    left join gold.supplier_acquisition_source main6 on main6.supplier_id = suppliers.id 
  GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6, 
    7, 
    8, 
    9, 
    10, 
    11, 
    12, 
    13, 
    14, 
    15, 
    16, 
    17, 
    18, 
    19, 
    20, 
    21, 
    22, 
    23, 
    24, 
    25, 
    26, 
    27, 
    28, 
    29, 
    30, 
    31
), 
CES as (
  SELECT 
    ds.date as dim__dt, 
    s.dim__supplier_id as dim__supplier_id 
  FROM 
    (
      SELECT 
        date 
      FROM 
        platinum.dim__calendar 
      where 
        date between CURRENT_DATE - interval '<START_DATE>' day 
        and CURRENT_DATE - interval '<END_DATE>' day
    ) ds 
    JOIN (
      select 
        id as dim__supplier_id, 
        date(created) as supplier_onboarding_date 
      from 
        silver.supply__suppliers 
      where 
        year >= 2015
    ) s ON ds.date >= s.supplier_onboarding_date
) 
SELECT 
  A.dim__dt as dim__dt, 
  a.dim__supplier_id as dim__supplier_id, 
  b.supplier_gstin as supplier_gstin, 
  b.supplier_gst_type as supplier_gst_type, 
  b.supplier_name as supplier_name, 
  b.supplier_valid as supplier_valid_flag, 
  b.supplier_city as supplier_city, 
  b.supplier_state as supplier_state, 
  b.region as supplier_region, 
  b.supplier_onboarding_date as supplier_onboarding_date, 
  b.supplier_qc_status as supplier_qc_status, 
  b.deactivation_date as supplier_deactivation_date, 
  b.deactivation_Reason as supplier_deactivation_reason, 
  b.supplier_phone as supplier_phone, 
  b.supplier_email as supplier_email, 
  b.supplier_bank_account_number as supplier_bank_account_number, 
  b.supplier_address_line as supplier_address, 
  b.is_supplier_international as supplier_international_flag, 
  --b.first_qc_status_change_date as first_qc_status_change_date,
  --b.first_aq_movement_date as first_aq_movement_date,
  b.supplier_pincode as supplier_pincode, 
  b.BD_name as bd_name, 
  b.BD_Email as bd_email, 
  b.BD_lead as bd_lead, 
  b.supplier_type as supplier_type, 
  b.deactivation_by as deactivation_by, 
  b.is_supplier_exclusive as meesho_exclusive_flag, 
  b.supplier_identifier as supplier_identifier, 
  b.meesho_trusted_supplier as meesho_trusted_supplier, 
  b.last_30_days_nps_score as last_30_days_nps_score, 
  b.supplier_acquisition_source as supplier_acquisition_source, 
  b.referral_flag as referral_flag 
from 
  CES As A 
  left join cte1 as B on A.dim__supplier_id = B.dim__supplier_id