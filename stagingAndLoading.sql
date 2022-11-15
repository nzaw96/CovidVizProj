--snowsql login
snowsql -a bab48039 -u nzaw96

create warehouse myFirstWh with warehouse_size='SMALL';

use warehouse myFirstWh;

create database COVID_DATABASE;

use covid_database;

create or replace file format my_csv_format
    type = 'csv'
    field_delimiter = ','
    skip_header = 1;

create or replace table epidem_states(
    date_record date primary key,
    location_key varchar(20),
    new_confirmed int,
    new_deceased int,
    new_recovered int,
    new_tested int,
    cumulative_confirmed int,
    cumulative_deceased int,
    cumulative_recovered int,
    cumulative_tested int,
    date_month varchar(10)   
);

--R

create or replace stage my_csv_stage
    file_format = my_csv_format;
    

show stages;

list @my_csv_stage; --nothing inside the stage currently

-- uploaded the file to the internal stage @my_csv_stage
put file:///Users/nayza/Desktop/CovidProj/epidem_states.csv @my_csv_stage 
--the similar syntax was used to stage the rest of the files from the local machine into @my_csv_stage via Snowsql

--copying the file from the internal stage into the defined table
copy into epidem_states
    from @my_csv_stage/epidem_states.csv.gz
    file_format = (format_name = my_csv_format)
    on_error = 'skip_file';
    
--validating that the file was copied into the table without errors
create or replace table save_copy_errors 
    as select * 
       from table(validate(demog_states, job_id=>'01a824a1-0001-82cb-0000-0005d48711f5'));
select * from save_copy_errors;

--Zero rows retuned from the preceding select statement, indicating that there were no errors when copying into table.
-- So, we drop the save_copy_errors tabel
drop table save_copy_errors;

--Verify that the data in the table looks like what it is supposed to look like.
select * from epidem_states
limit 5;

--The first csv file was successfully staged and copied into the table.
--Let's do the same for the rest of the files.
create table epidem_US like epidem_states;

copy into epidem_US
    from @my_csv_stage/epidem_US.csv.gz
    file_format = (format_name = my_csv_format)
    on_error = 'skip_file';
    
    
create or replace table demog_states(
    location_key varchar(20) primary key,
    population bigint,
    population_male int,
    population_female int,
    population_age_00_09 int,
    population_age_10_19 int,
    population_age_20_29 int,
    population_age_30_39 int,
    population_age_40_49 int,
    population_age_50_59 int,
    population_age_60_69 int,
    population_age_70_79 int,
    population_age_80_and_older int
);


copy into demog_states
    from @my_csv_stage/demog_states.csv.gz
    file_format = (format_name = my_csv_format)
    on_error = 'skip_file';


--demog_US.csv has six extra columns compared to demog_states.csv file, therefore
--a new table will be created for demog_US entirely instead of copying the table structure
--of demog_states table.
create or replace table demog_US(
    location_key varchar(20) primary key,
    population bigint,
    population_male bigint,
    population_female bigint,
    population_rural bigint,
    population_urban bigint,
    population_largest_city bigint,
    population_clustered bigint,
    population_density smallint,
    human_development_index smallint,
    population_age_00_09 bigint,
    population_age_10_19 bigint,
    population_age_20_29 bigint,
    population_age_30_39 bigint,
    population_age_40_49 bigint,
    population_age_50_59 bigint,
    population_age_60_69 bigint,
    population_age_70_79 bigint,
    population_age_80_and_older bigint
);


create or replace table emergency_declarations(
    date_record date,
    location_key varchar(20),
    mitigation_policy tinyint,
    emerg_statewide tinyint,
    stayHome_statewide tinyint,
    mask_statewide tinyint,
    schoolsClosed_statewide tinyint,
    gatheringBanned_statewide tinyint,
    date_month varchar(10)
);

copy into emergency_declarations
   from @my_csv_stage/emerg_declarations.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';
   
create table govResp_states(
    date_record date,
    location_key varchar(20),
    school_closing tinyint,
    workplace_closing tinyint,
    cancel_public_events tinyint,
    restrictions_on_gatherings tinyint,
    public_transport_closing tinyint,
    stay_at_home_requirements tinyint,
    restrictions_on_internal_movement tinyint,
    international_travel_controls tinyint,
    income_support bigint,
    public_info_campaigns tinyint,
    testing_policy tinyint,
    contact_tracing tinyint,
    emergency_healthcare_investment bigint,
    vaccine_investment bigint,
    mask_requirement tinyint,
    vaccination_policy tinyint,
    stringency_index decimal,
    date_month varchar(10)
);
    

copy into govResp_states
   from @my_csv_stage/govResp_states.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';
   
create table govResp_US like govResp_states;

copy into govResp_US
   from @my_csv_stage/govResp_US.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';


create or replace table hospitalized_states(
    date_record date,
    location_key varchar(20),
    new_hospitalized_patients int,
    cumulative_hospitalized_patients int,
    current_hospitalized_patients int,
    new_intensive_care_patients int,
    cumulative_intensive_care_patients int,
    current_intensive_care_patients int,
    new_ventilator_patients int,
    cumulative_ventilator_patients int,
    current_ventilator_patients int,
    date_month varchar(10)
);

copy into hospitalized_states
   from @my_csv_stage/hospitalized_states.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';
   
create or replace table hospitalized_US like hospitalized_states;

copy into hospitalized_US
   from @my_csv_stage/hospitalized_US.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';

--deleting the current files mobility_states.csv and mobility_US.csv from the stage
--and re-uploading the updated ones
rm @my_csv_stage pattern='.*mobility.*';
put file:///Users/nayza/Desktop/CovidProj/mobility_states.csv @my_csv_stage
put file:///Users/nayza/Desktop/CovidProj/mobility_US.csv @my_csv_stage 

--All the decimal type columns in the table represent percentage change in amount/frequency compared to
--pre-defined baseline.
create or replace table mobility_states(
    date_record date,
    location_key varchar(20),
    retail_and_recreation decimal,
    grocery_and_pharmacy decimal,
    parks decimal,
    transit_stations decimal,	
    workplaces decimal,
    residential decimal,
    date_month varchar(10)
);

copy into mobility_states
   from @my_csv_stage/mobility_states.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';
   
create or replace table mobility_US like mobility_states;

copy into mobility_US
   from @my_csv_stage/mobility_US.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';

create or replace table vaccination_states(
    date_record date,
    location_key varchar(20),
    new_persons_vaccinated bigint,
    cumulative_persons_vaccinated bigint,
    new_persons_fully_vaccinated bigint,
    cumulative_persons_fully_vaccinated bigint,
    new_vaccine_doses_administered bigint,
    cumulative_vaccine_doses_administered bigint,
    date_month varchar(10)
);

copy into vaccination_states
   from @my_csv_stage/vaccination_states.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';
   
create or replace table vaccination_US like vaccination_states;

copy into vaccination_US
   from @my_csv_stage/vaccination_US.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';
   
desc table epidem_US;

--Initially set date_record as the primary key but quickly realized that data_record values aren't unique.
select date_record from epidem_states
where date_record = '2020-03-20'; -- this query returns 56 rows!

alter table epidem_states
modify column date_record date;--this somehow doesn't work.

alter table epidem_states
drop primary key;--this worked, yay!

desc table epidem_states;

select count(distinct date_record) from epidem_US;
select count(date_record) from epidem_US;
--both of above queries return 988 which means it's fine leaving date_record as primary key column for
--epidem_US table but we'll drop the primary key constraint for it as well and reassess later.

alter table epidem_US
drop primary key;

--finally removed all files in the internal stage my_csv_stage to avoid incurring unnecessary costs.
rm @my_csv_stage/;

--adding a new file epidem_states_daily.csv to my_csv_stage
put file:///Users/nayza/Desktop/CovidProj/epidem_states_daily.csv @my_csv_stage 

--creating a table for the new file: epidem_states_daily.csv
create table epidem_states_daily (
    date_record date,
    location_key varchar(20),
    new_confirmed int,
    new_deceased int,
    new_tested int
);

--copying the new file from the internal stage into the table
copy into epidem_states_daily
   from @my_csv_stage/epidem_states_daily.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';


--creating another table for epidem_US_daily.csv
create table epidem_US_daily like epidem_states_daily;

put file:///Users/nayza/Desktop/CovidProj/epidem_US_daily.csv @my_csv_stage; 

copy into epidem_US_daily
   from @my_csv_stage/epidem_US_daily.csv.gz
   file_format = (format_name = my_csv_format)
   on_error = 'skip_file';

--checking contents of epidem_US_daily table
select * from epidem_US_daily;
