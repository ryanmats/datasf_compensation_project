with source as (
    select * from {{ source('src_employee_compensation_raw', 'EMPLOYEE_COMPENSATION') }}
),


-- Rename source variables and cast them to the desired types.
renamed_and_cast as (
    select
        /* Here we generate a unique identifier for each employee by finding unique employee_name,
        department_code, and job_family_code combinations. This is necessary for records from 2017
        and onwards (where employee_name is just their name), but works for 2013-2016 as well (where
        employee_name is already a 4 digit numberical unique identifier) */
        {{ dbt_utils.generate_surrogate_key([
            'EMPLOYEE_NAME', 
            'DEPARTMENT_CODE', 
            'JOB_FAMILY_CODE'
        ]) }} as employee_id,

        /* Employee fields. Note that employee_name for 2013-2016 records is a 4 digit unique identifier
        but for 2017-2025 records is their actual name. */
        employee_name::varchar as employee_name,
        employment_type::varchar as employment_type,

        -- Year fields. We have 2013-2025 in our dataset and Calendar/Fiscal for year type.
        "YEAR"::int as "YEAR",
        year_type::varchar as year_type,

        -- Important fields that tell us about someone's organization/department/job family/job role/union
        organization_group_code::varchar as organization_group_code,
        organization_group::varchar as organization_group,
        department_code::varchar as department_code,
        department::varchar as department,
        job_family::varchar as job_family,
        job_family_code::varchar as job_family_code,
        job_code::varchar as job_code,
        job::varchar as job,
        union_code::varchar as union_code,
        "Union"::varchar as union_name,
        
        -- Compensation Data (Removing commas and cast to numbers).
        replace(salaries, ',', '')::number(38, 2) as salaries,
        replace(overtime, ',', '')::number(38, 2) as overtime,
        replace(other_salaries, ',', '')::number(38, 2) as other_salaries,
        replace(total_salary, ',', '')::number(38, 2) as total_salary,
        replace(retirement, ',', '')::number(38, 2) as retirement,
        replace(health_and_dental, ',', '')::number(38, 2) as health_and_dental,
        replace(other_benefits, ',', '')::number(38, 2) as other_benefits,
        replace(total_benefits, ',', '')::number(38, 2) as total_benefits,
        replace(total_compensation, ',', '')::number(38, 2) as total_compensation,
        replace(hours, ',', '')::number(38, 2) as hours,

        -- Last Updated Data
        to_timestamp(data_as_of, 'YYYY Mon DD HH12:MI:SS AM') as data_as_of,
        to_timestamp(data_loaded_at, 'YYYY/MM/DD HH12:MI:SS AM') as data_loaded_at
    from source
),

/* Deduplication step. Here we look for duplicate rows based on certain key fields matching.
We take the most recently loaded/updated row if we find duplicates. */
deduplicated_table as (
    select * from renamed_and_cast
    qualify row_number() over (
        partition by "YEAR", year_type, employee_name, department_code, job_family_code, job_code
        order by data_loaded_at desc, data_as_of desc
    ) = 1
)

select * from deduplicated_table