-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##SQL IMPLEMENTATION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Scenario 1 - The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

-- COMMAND ----------

select count(*) as distinct_studies from(
    select
      distinct Id
    from
      default.clinicaltrial_2023
)a

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Scenario 2 - You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

select
  case when Type is null or Type='' then 'None' else Type end as Type,
  count(Id) as Id_Count
from
  default.clinicaltrial_2023
where
  Type is not null
  or trim(Type) != ''
group by
  Type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Scenario 3 - The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

with explode_conditions as (
SELECT ID,explode(SPLIT(Conditions, '[|]')) as Conditions_Final from clinicaltrial_2023
)

select
  distinct *
from(
    select Conditions_Final,count(Conditions_Final) as conditions_count from explode_conditions group by Conditions_Final
  ) a qualify dense_rank() over (
    order by
      conditions_count desc
  ) <= 6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Scenario 4 - Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored.

-- COMMAND ----------

with remove_parent_sponsors as(
  select *
  from(
      select distinct trim(Sponsor) as Sponsor
      from default.clinicaltrial_2023
      where  Sponsor is not null or trim(Sponsor) != ''
      except
      select  distinct trim(Parent_Company) as Parent_Company
      from default.pharma
      where Parent_Company is not null or trim(Parent_Company) != ''
    ) a)
select
  distinct Sponsor,
  count(*) over(partition by Sponsor order by Sponsor) as Trial_count
from
  default.clinicaltrial_2023
where
  trim(Sponsor) in (select Sponsor from remove_parent_sponsors) qualify dense_rank() over (
    order by trial_count desc) <= 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Scenario 5 - Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month.

-- COMMAND ----------

select
  distinct 
  lpad(month(to_date(trim(Completion))), 2, 0) as Month,
  count(*) over (
    partition by month(to_date(trim(Completion)))
    order by
      month(to_date(trim(Completion))) desc
  ) as Completion_Count
from
  default.clinicaltrial_2023
where
  lower(Status) = "completed"
  and year(to_date(Completion)) = '2023'
order by
  Month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Additional - Completion status year wise

-- COMMAND ----------

select
  distinct lpad(year(to_date(trim(Completion))), 4, 0) as Year,
  count(*) over (
    partition by year(to_date(trim(Completion)))
    order by
      year(to_date(Completion)) desc
  ) as Completion_Count
from
  default.clinicaltrial_2023
where
  lower(Status) = "completed"
order by
  Year desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Additional scenario to validate - top 10 Sponsors without any filter

-- COMMAND ----------

select
  distinct Sponsor,
  count(*) over(
    partition by Sponsor
    order by
      Sponsor
  ) as trial_count
from
  default.clinicaltrial_2023
where
  trim(Sponsor) !='' qualify dense_rank() over (
    order by
      trial_count desc
  ) <= 10
  --Top 10 sponsors without any filter

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Additional Scenario - To Validate Patient with most number of Conditions

-- COMMAND ----------

SELECT ID, count(Conditions_Final) as cnt from(
SELECT ID,explode(SPLIT(Conditions, '[|]')) as Conditions_Final from clinicaltrial_2023
)a group by ID order by cnt desc

-- COMMAND ----------

SELECT ID,Conditions,explode(SPLIT(Conditions, '[|]')) as Conditions_Final from clinicaltrial_2023 where Id = 'NCT01793168'

-- COMMAND ----------

with explode_conditions as (
SELECT ID,explode(SPLIT(Conditions, '[|]')) as Conditions_Final from clinicaltrial_2023
)

select
  distinct *
from(
    select Conditions_Final,count(Conditions_Final) as conditions_count from explode_conditions group by Conditions_Final
  ) a qualify dense_rank() over (
    order by
      conditions_count
  ) <= 5;
