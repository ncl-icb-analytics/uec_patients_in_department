CREATE TABLE YourTableName (
    date_weekstarting DATE NOT NULL,
    date_weekending DATE NOT NULL,
    fin_year CHAR(7) NOT NULL,
    month CHAR(3) NOT NULL,
    site_code CHAR(5) NOT NULL,
    patients_mean FLOAT NOT NULL,
    arrivals_mean FLOAT NOT NULL,
    completeness FLOAT NOT NULL,
    CONSTRAINT PK_ecds_patients_at_site PRIMARY KEY (date_weekstarting, site_code)
);
