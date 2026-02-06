terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 1.0"
    }
  }
}

# -------------------------------------------------------------------
# PROVIDER CONFIGURATION
# -------------------------------------------------------------------
# Use variables for sensitive data to avoid hardcoding credentials.
provider "snowflake" {
  organization_name = "INSERT_ORGANIZATION_NAME_HERE"
  account_name      = "INSERT_ACCOUNT_NAME_HERE"
  user              = "INSERT_USER_HERE"
  password          = var.snowflake_password 
  role              = "ACCOUNTADMIN"
}

variable "snowflake_password" {
  type      = string
  sensitive = true
  default   = "YOUR_PASSWORD_HERE"
}

# -------------------------------------------------------------------
# 1. ROLES & HIERARCHY
# -------------------------------------------------------------------

resource "snowflake_account_role" "hr_analyst" {
  name = "HR_ANALYST"
}

resource "snowflake_account_role" "auditor" {
  name = "AUDITOR"
}

resource "snowflake_account_role" "public_access" {
  name = "PUBLIC_ACCESS"
}

# Granting roles to the current user for UI visibility and testing
resource "snowflake_grant_account_role" "user_grants" {
  for_each  = toset(["HR_ANALYST", "AUDITOR", "PUBLIC_ACCESS"])
  role_name = each.value
  user_name = "YOUR_USERNAME_HERE"
}

# -------------------------------------------------------------------
# 2. PRIVILEGES (Database, Schema, Table)
# -------------------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "db_usage" {
  for_each          = toset(["HR_ANALYST", "AUDITOR", "PUBLIC_ACCESS"])
  privileges        = ["USAGE"]
  account_role_name = each.value
  on_account_object {
    object_type = "DATABASE"
    object_name = "EMPLOYEE_COMPENSATION"
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  for_each          = toset(["HR_ANALYST", "AUDITOR", "PUBLIC_ACCESS"])
  privileges        = ["USAGE"]
  account_role_name = each.value
  on_schema {
    schema_name = "EMPLOYEE_COMPENSATION.PUBLIC"
  }
}

# Only give HR_ANALYST and AUDITOR access to reporting table. PUBLIC_ACCESS is not allowed to see this table (they have their own view that is defined in Step #5).
resource "snowflake_grant_privileges_to_account_role" "table_select" {
  for_each          = toset(["HR_ANALYST", "AUDITOR"])
  privileges        = ["SELECT"]
  account_role_name = each.value
  on_schema_object {
    object_type = "TABLE"
    object_name = "EMPLOYEE_COMPENSATION.PUBLIC.REPORTING_COMPENSATION_BY_TENURE"
  }
}

# -------------------------------------------------------------------
# 3. GOVERNANCE POLICIES (Dynamic Masking & Row-Level Security)
# -------------------------------------------------------------------

resource "snowflake_masking_policy" "name_mask" {
  name               = "EMPLOYEE_NAME_MASK"
  database           = "EMPLOYEE_COMPENSATION"
  schema             = "PUBLIC"
  return_data_type   = "VARCHAR"
  argument {
    name = "val"
    type = "VARCHAR"
  }
  # Logic: Only Auditor and Admin bypass redaction.
  # IS_ROLE_IN_SESSION is used to support role inheritance and complex session contexts.
  body = "CASE WHEN IS_ROLE_IN_SESSION('AUDITOR') OR IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val ELSE 'REDACTED' END"
}

resource "snowflake_row_access_policy" "dept_policy" {
  name     = "DEPT_SECURITY_POLICY"
  database = "EMPLOYEE_COMPENSATION"
  schema   = "PUBLIC"
  argument {
    name = "department"
    type = "VARCHAR"
  }
  # Logic: HR Analyst/Admin see all. Auditor sees only 'Human Resources'. 
  # Public Access is included here to allow data to flow through the Secure View aggregation.
  body = "IS_ROLE_IN_SESSION('ACCOUNTADMIN') OR IS_ROLE_IN_SESSION('HR_ANALYST') OR IS_ROLE_IN_SESSION('PUBLIC_ACCESS') OR (IS_ROLE_IN_SESSION('AUDITOR') AND department = 'Human Resources')"
}

# -------------------------------------------------------------------
# 4. POLICY APPLICATION (The Bridge)
# -------------------------------------------------------------------
# Note: Snowflake masking/row policies on tables are not yet fully natively 
# supported as standalone resources in the provider for existing dbt-managed tables.
# We utilize null_resource with local-exec to ensure post-deployment attachment.

resource "null_resource" "apply_policies" {
  triggers = {
    policy_id = snowflake_masking_policy.name_mask.id
    row_id    = snowflake_row_access_policy.dept_policy.id
  }

  depends_on = [
    snowflake_masking_policy.name_mask, 
    snowflake_row_access_policy.dept_policy,
    snowflake_grant_privileges_to_account_role.table_select
  ]

  provisioner "local-exec" {
    command = <<EOT
      snow sql -c default -q "ALTER TABLE EMPLOYEE_COMPENSATION.PUBLIC.REPORTING_COMPENSATION_BY_TENURE MODIFY COLUMN employee_name UNSET MASKING POLICY;" || true
      snow sql -c default -q "ALTER TABLE EMPLOYEE_COMPENSATION.PUBLIC.REPORTING_COMPENSATION_BY_TENURE DROP ALL ROW ACCESS POLICIES;" || true
      snow sql -c default -q "ALTER TABLE EMPLOYEE_COMPENSATION.PUBLIC.REPORTING_COMPENSATION_BY_TENURE MODIFY COLUMN employee_name SET MASKING POLICY EMPLOYEE_COMPENSATION.PUBLIC.EMPLOYEE_NAME_MASK;"
      snow sql -c default -q "ALTER TABLE EMPLOYEE_COMPENSATION.PUBLIC.REPORTING_COMPENSATION_BY_TENURE ADD ROW ACCESS POLICY EMPLOYEE_COMPENSATION.PUBLIC.DEPT_SECURITY_POLICY ON (department);"
EOT
  }
}

# -------------------------------------------------------------------
# 5. DATA ABSTRACTION LAYER (Secure Views)
# -------------------------------------------------------------------

resource "snowflake_view" "public_summary" {
  database  = "EMPLOYEE_COMPENSATION"
  schema    = "PUBLIC"
  name      = "COMPENSATION_SUMMARY_BY_JOB_FAMILY"
  is_secure = true
  statement = <<-SQL
    SELECT job_family, SUM(total_compensation_calculated) as total_pay
    FROM EMPLOYEE_COMPENSATION.PUBLIC.REPORTING_COMPENSATION_BY_TENURE
    GROUP BY 1
  SQL
}

resource "snowflake_grant_privileges_to_account_role" "view_select" {
  privileges        = ["SELECT"]
  account_role_name = "PUBLIC_ACCESS"
  on_schema_object {
    object_type = "VIEW"
    object_name = "EMPLOYEE_COMPENSATION.PUBLIC.COMPENSATION_SUMMARY_BY_JOB_FAMILY"
  }
}