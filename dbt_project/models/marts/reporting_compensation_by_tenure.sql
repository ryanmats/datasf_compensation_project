WITH base_data AS (
    SELECT * FROM {{ ref('stg_employee_compensation') }}
),


/* Step #1: Find every fiscal year that each
   employee has worked (using employee_id, which
   segments by employee_name, department_code,
   and job_family_code) */
fiscal_years_per_employee AS (
    SELECT DISTINCT
        employee_id,
        "YEAR" as fiscal_year
    FROM base_data
    WHERE year_type = 'Fiscal'
),

/* Step #2: Calculate what number fiscal year it is
   for an employee at a given fiscal year they worked.
   For example, if Kevin Lee started work in FY2018,
   FY2019 would be fiscal year #2 for him. */
experience_mapping AS (
    SELECT
        employee_id,
        fiscal_year,
        ROW_NUMBER() OVER (
            PARTITION BY employee_id 
            ORDER BY fiscal_year ASC
        ) AS fiscal_year_count
    FROM fiscal_years_per_employee
),

/* Step 3: Join fiscal years of experience to our base table (which contains data for both Calendar
   and Fiscal Years.
   - We make sure that fiscal years of experience is always 0 or greater (to account
   for the edge case where an employee started in December 2017, which is Calendar Year 2017 but Fiscal
   Year 2018. The reason we count fiscal years of experience is because that is how the City of SF
   determines seniority.
   - Important Note: For years 2013-2016, employee_name is a 4 digit numerical unique identifier. But
   for 2017-2025, employee_name is their actual name as a string. This prevents us from calculating
   years of experience across the entire time period, since we do not have the mapping from the pre-2017
   IDs to the employee names from 2017 and later. We should do some investigation to see if we can find
   this mapping somewhere.
   - As of now, this reporting table acts as if there is an entirely different set of employees from 2013-2016
   versus 2017-2025.
   */
final_calculations AS (
    SELECT
        b.employee_id,
        b.employee_name,
        b.department,
        b.job_family,
        b.job,
        b.year,
        b.year_type,
        b.salaries,
        b.overtime,
        b.other_salaries,
        b.total_salary,
        (b.salaries + b.overtime + b.other_salaries) as total_compensation_calculated,

        -- If there's no matching fiscal year yet (like Dec 2017), it's 0.
        -- If it's the 1st fiscal year (1 - 1), it's 0.
        -- GREATEST ensures we never return a negative number.
        GREATEST(COALESCE(m.fiscal_year_count, 1) - 1, 0) AS years_experience
    FROM base_data b
    LEFT JOIN experience_mapping m
        ON b.employee_id = m.employee_id
        AND b."YEAR" = m.FISCAL_YEAR
)


-- Step #4: Bracket years_experience into 5 different buckets.
SELECT 
    *,
    CASE 
        WHEN years_experience = 0 THEN '< 1 Year'
        WHEN years_experience BETWEEN 1 AND 2 THEN '1 - 2 Years'
        WHEN years_experience BETWEEN 3 AND 5 THEN '3 - 5 Years'
        WHEN years_experience BETWEEN 6 AND 9 THEN '6 - 9 Years'
        ELSE '10+ Years'
    END AS tenure_bracket
FROM final_calculations

