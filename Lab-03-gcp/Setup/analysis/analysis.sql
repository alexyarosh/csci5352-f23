---------- Q1: How many animals of each type have outcomes? 
-- Note the count distinct: some animals had multiple outcomes
-- Also note the join with fct_outcomes: 
-- there's a priori no guarantee that every animal in the database had an outcome 
SELECT animal_type, count(distinct animal_id) 
from fct_outcomes join dim_animals using (animal_id)
group by animal_type;

---------- Q2: How many animals are there with more than 1 outcome?

-- This query will return all animals who have more than one outcome. 
-- This is acceptable 
SELECT animal_id, count(*) as ct
from fct_outcomes join dim_animals using (animal_id)
group by animal_id
having count(*) > 1;

-- The query below will return actual count of animals:
select count(*) from (
  SELECT animal_id, count(*) as ct
  from fct_outcomes join dim_animals using (animal_id)
  group by animal_id
  having count(*) > 1
  );
  
----------- Q3: What are the top 5 months for outcomes?  
 
SELECT month, count(*) as ct
from fct_outcomes join dim_dates using (date_id)
group by month
order by ct desc 
limit 5;

----------- Q4(a): A "Kitten" is a "Cat" who is less than 1 year old. 
--------------- A "Senior cat" is a "Cat" who is over 10 years old. 
--------------- An "Adult" is a cat who is between 1 and 10 years old.
--------------- What is the total number of kittens, adults, and seniors, whose outcome is "Adopted"?
select 
    case 
    	when (date-dob)/365 < 1 then 'Kitten'
    	when (date-dob)/365 > 10 then 'Senior'
    	else 'Adult'
    end as age,
    count(*) as ct_adopted
from fct_outcomes
     join dim_animals using(animal_id) 
     join dim_dates using(date_id)
     join dim_outcome_types using(outcome_type_id)
where outcome_type = 'Adoption'     
group by age;

--- orignal version of the problem asked for percentage instead of total. 
--- That's slighly more complicated
--- There are multiple ways to accomplish this: with partitions, subqueries..
--- Code below is using CTEs

with outcomes_by_age as (
  select 
      case 
      	  when (date-dob)/365 < 1 then 'Kitten'
    	  when (date-dob)/365 > 10 then 'Senior'
    	  else 'Adult'
      end as age,
      outcome_type,
      count(*) as ct_outcomes
  from fct_outcomes
       join dim_animals using(animal_id) 
       join dim_dates using(date_id)
       join dim_outcome_types using(outcome_type_id)  
  group by age, outcome_type
),

totals_by_age as (
   select age, sum(ct_outcomes) as ttl
   from outcomes_by_age
   group by age
),

adoptions_by_age as (
   select age, sum(ct_outcomes) as adopted
   from outcomes_by_age
   where outcome_type = 'Adoption'
   group by age
)

select age, ttl, adopted, adopted/ttl as pct_adopted
from totals_by_age join adoptions_by_age using (age)



----------- Q4(b): Among all the cats who were "Adopted", what is the total number of kittens, adults, and seniors?
---- Note: after I changed "percentage" to "total number", this question is identical to Q4(a)
---- Percentage computation would be different, however. See below for an example:

with outcomes_by_age as (
  select 
      case 
      	  when (date-dob)/365 < 1 then 'Kitten'
    	  when (date-dob)/365 > 10 then 'Senior'
    	  else 'Adult'
      end as age,
      outcome_type,
      count(*) as ct_outcomes
  from fct_outcomes
       join dim_animals using(animal_id) 
       join dim_dates using(date_id)
       join dim_outcome_types using(outcome_type_id)  
  group by age, outcome_type
),

total_adoptions as (
   select sum(ct_outcomes) as ttl
   from outcomes_by_age
   where outcome_type = 'Adoption'
   group by outcome_type
),

by_age_adoptions as (
   select age, sum(ct_outcomes) as age_ct
   from outcomes_by_age
   where outcome_type = 'Adoption'
   group by age
)

select age, age_ct, ttl, age_ct/ttl as age_pct
from by_age_adoptions, total_adoptions;


------------------------ Q5: For each date, what is the cumulative total of outcomes up to and including this date?
select distinct date, count(*) over (ORDER BY date)
from fct_outcomes join dim_dates using (date_id)
order by date asc;


