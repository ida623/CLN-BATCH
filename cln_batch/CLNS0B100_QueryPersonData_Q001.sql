with PERSON_DATA as (
    select
        CCI.*,
        CCS.SERV_ADDR,
        CCS.SERV_NAME,
        CCS.BUSINESS_CODE,
        CCS.JOB_TITLE,
        CCS.YEAR_INCOME,
        CCS.SERV_MONTH,
        CCS.SERV_CORPID,
        CPC_CON.CITY_NAME CON_CITY_NAME,
        CPC_CON.TOWN_NAME CON_TOWN_NAME,
        CPC_REG.CITY_NAME REG_CITY_NAME,
        CPC_REG.TOWN_NAME REG_TOWN_NAME,
        case
            when CCM.SIGN_DATE is not null then CCM.SIGN_DATE
            else CCM.APPLY_DATE
        end DATA_DATE
    from CLN_CUSTOMER_INFO CCI
    join CLN_CASE_MAIN CCM
        on CCI.APPLY_ID = CCM.APPLY_ID
    join CLN_POSTAL_CODE CPC_CON
        on substr(CCI.CON_ZIP, 1, 3) = CPC_CON.POSTCODE
    join CLN_POSTAL_CODE CPC_REG
        on substr(CCI.REG_ZIP, 1, 3) = CPC_REG.POSTCODE
    left join (
        select
            CCST.*,
            row_number() over (
                partition by CCST.APPLY_ID, CCST.CUSTOMER_TYPE, CCST.CUSTOMER_ID
                order by SEQ_NO desc
            ) as R
        from CLN_CUSTOMER_SERVICE CCST
    ) CCS
        on CCS.APPLY_ID = CCI.APPLY_ID
        and CCS.CUSTOMER_ID = CCI.CUSTOMER_ID
        and CCS.CUSTOMER_TYPE = CCI.CUSTOMER_TYPE
        and R = 1
    where CCM.LOAN_TYPE in ('1', '2', '3', '4')
      and (CCI.CUSTOMER_TYPE = '0' or (CCI.CUSTOMER_TYPE = '1' and CCI.GUAR_KIND in ('1', '2', '3')))
      and to_number(CCM.APPLY_PROCESS) >= 20
      and to_number(CCM.APPLY_PROCESS) <= 90
)
select *
from (
    select
        DATA.*,
        row_number() over (partition by CUSTOMER_ID order by DATA_DATE desc) as R
    from PERSON_DATA DATA
    join (
        select distinct trim(CUSTOMER_ID) as CUSTOMER_ID
        from LONLXLIP01CLN.CLN_CUSTOMER_ID_TEMP
    ) CIT
        on CIT.CUSTOMER_ID = DATA.CUSTOMER_ID
) RESULT
where R = 1