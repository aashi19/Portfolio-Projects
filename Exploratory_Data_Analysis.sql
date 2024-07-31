-- Exploratory Data Analysis

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!

select *
from 
	layoffs_staging2;
 
 -- trying to see what is the max number of employees that got laid off
select
	max(total_laid_off)
from 
	layoffs_staging2;
    
select *
from
	layoffs_staging2
where
	total_laid_off = 12000;
    
-- Looking at Percentage to see how big these layoffs were
select
	max(percentage_laid_off), min(percentage_laid_off)
from
	layoffs_staging2
where
	percentage_laid_off is not null;
    
-- The companies that had 1 which is basically 100 percent of the company laid off

-- I'll check for the company that was 100% laid off
select *
from 
	layoffs_staging2
where
	percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
select *
from 
	layoffs_staging2
where
	percentage_laid_off = 1
order by funds_raised_millions desc;

-- lets check by company names how many people were laid off
select
	company, sum(total_laid_off)
from
	layoffs_staging2
group by
	company
order by
	2 desc;
    
select 
	min(`date`), max(`date`)
from
	layoffs_staging2;
    
select
	industry, sum(total_laid_off)
from
	layoffs_staging2
group by
	industry
order by
	2 desc;
    
select
	country, sum(total_laid_off)
from
	layoffs_staging2
group by
	country
order by
	2 desc;
    
select
	year(`date`), sum(total_laid_off)
from
	layoffs_staging2
group by
	year(`date`)
order by
	1 desc;
    
select
	stage, sum(total_laid_off)
from
	layoffs_staging2
group by
	stage
order by
	2 desc;
    
-- Lets find rolling total layoff
select substring(`date`,6,2) as `MONTH`
from layoffs_staging2;

select *
from layoffs_staging2;
 
select substring(`date`,1,7) as `year_month` , sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `year_month`
order by `year_month` asc;

with rolling_total as
(
select substring(`date`,1,7) as `year_month` , sum(total_laid_off) as laid_off_by_months
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `year_month`
order by `year_month` asc
)
select `year_month`, laid_off_by_months, sum(laid_off_by_months) over(order by `year_month`) as total_laid
from rolling_total;


select company, YEAR(`date`) , sum(total_laid_off) 
from layoffs_staging2
group by company,YEAR(`date`)
order by company asc;

with Company_Year (company, years, total_laid_off) as					#first CTE
(
	select company, YEAR(`date`), sum(total_laid_off)
    from layoffs_staging2
    group by company, YEAR(`date`)
   ), Company_Year_Rank as			#Second CTE
	(select *, dense_rank() OVER(partition by years order by total_laid_off desc) as ranking
	from Company_Year
	where years is not null)
select *
from Company_Year_Rank
where ranking <= 5;
;
    