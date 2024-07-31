-- Data Cleaning

select *
from layoffs;

-- Now we are going to perform following steps as a part of data cleaning
-- 1. Remove duplicates
-- 2. Standardise the data
-- 3. Treat / Remove null values and blank values
-- 4. Remove any column that is irrelevant
-- It is best practice to not remove anything from the raw data set, because if we do that we might be missing on some important information. Do avoid that it is advised to stage the dataset i.e create a copy of original data set and work on that

-- Staging the data
Create table layoffs_staging
like layoffs;

select *
from layoffs_staging;  #by doing this you have created exactly the same schema and properties as the base/ main table


-- Now we will insert the data from the main table to the staging table
insert into layoffs_staging
select *
from layoffs;

select *
from layoffs_staging;

-- We will start with the first step of finding the duplicate values in the dataset. For this we are going to create CTE's
with duplicate_cte as
(
	select *,
    row_number() over(    						#by applying row number function we are determing the number of rows that have duplicate values 
    Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
    from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- once we find the number of rows that are having the dupliate rows the next task is to remove/ delete those rows from the database. For this we cannot add a DELETE to our CTE because CTE's does not support update
 -- In order to delete the duplicate rows we are going to make another staging table and we will add row_num also to that column
 
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
    row_number() over(    						#by applying row number function we are determing the number of rows that have duplicate values 
    Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
    from layoffs_staging;
    
select *
from layoffs_staging2
where row_num > 1;   

delete
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2;

select *
from layoffs_staging2
where row_num > 1; 

-- Standardization
-- One way to start standardizing the data is removing the white spaces from the columns

select company, trim(company)
from layoffs_staging2;

-- once you standardize something you have to push those updates into the table
Update layoffs_staging2
set company = trim(company);

-- another way to standardise data is to look for same/similar values in the different rows of the column. For eg. here we can check if there are any industry who have same ame but they are written differently
select distinct industry
from layoffs_staging2
order by 1;

-- we can notice that Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry is null
or industry = ''
order by industry;

select *
from layoffs_staging2
where company like 'Airbnb%';

select *
from layoffs_staging2
where company like 'Bally%';

select *
from layoffs_staging2
where company like 'Carvana%';

select *
from layoffs_staging2
where company like 'Juul%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
update layoffs_staging2
set industry = null
where industry like '';

-- now if we check those are all null
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible
-- here we will perfornm self join between the table
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

select distinct country
from layoffs_staging2
order by 1;
 -- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this. 

update layoffs_staging2
set country = trim(trailing '.' from country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- if we want to use this data in future, for time series related prediction or visualisation, date needs to be in the same format for every value
-- we will use str_to_date to convert the text into date and it will convert the value into standard sql date format
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- we need to change the datatype of the column date in order to get no errors
-- always do this on the staging table never do this on the actual table

alter table layoffs_staging2
modify column `date` DATE;

-- now we will check for null values
select *
from layoffs_staging2;


-- 3. Look at Null Values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values

-- looks like  column total_laid_off and percentage_laid_off has the maximum null values in it
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- 4. remove any columns and rows we need to
delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
