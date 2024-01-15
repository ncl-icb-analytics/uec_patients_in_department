# UEC Patients in Department

This is a python script to maintain and update the [Data_Lab_NCL_Dev].[JakeK].[uec_patients_in_department_dev] table in the SQL Sandpit. This table contains the mean number of patients in A&E conveyed via ambulance and the mean number of arrivals to A&E per hour.

The intention is the run the script monthly to update the table with new data.

[UEC Patients in Department](#uec-patients-in-department)

## History ## 
### [1.0] - 15/01/2024 ###
* Initial code from base request

## Output ##
This an explanation of the columns in the output table:

**date_weekstarting**: _Date_ \
The date of Monday (start of week) for a given week in the table

**date_weekending**: _Date_ \
The date of Sunday (end of week) for a given week in the table

**fin_year**: _Char(7)_ \
The financial year for a given week (row) in the format yyyy-zz

**month**: _Char(3)_ \
The month for a given week (row) in the format mmm

**site_code**: _Char(5)_ \
The code for the site. Sites include "RAL26", "RKEQ4", "RAL01", "RRV03", "RAPNM".

**patients_mean**: _Float_ \
The mean number of patients (per hour) in A&E that were conveyed via ambulance for a given week and site. A patient is considered to be in A&E for a given hour if they spend any amount of time in A&E during that hour.

**arrivals_mean**: _Float_ \
The mean number of ambulance arrivals for a given week and site.

**completeness**: _Float_ \
The percentage of days in a week where there is data for a given site. A value of 1 implies that every hour for a given week and site has a record of at least 1 patient in A&E. A value <1 may imply there were no patients in A&E for some period instead of a data quality issue.

