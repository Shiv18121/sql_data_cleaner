-- Removing Duplicates

create schema world_layoffs;
use world_layoffs;
select * from layoffs;
create table layoffs_staging like layoffs;
insert into layoffs_staging select * from layoffs;
select * from layoffs_staging;
select *, row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging;
with cte_duplicates as 
(
select *, row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * from cte_duplicates where row_num>1;
create table layoffs_staging2 like layoffs_staging;
alter table layoffs_staging2 add row_num int;
insert into layoffs_staging2 
select *, row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging;
select * from layoffs_staging2 where row_num>1;
delete from layoffs_staging2 where row_num>1;

-- standardizing data

update layoffs_staging2 set company=trim(company);
select * from layoffs_staging2;
select distinct industry from layoffs_staging2 order by 1;
select * from layoffs_staging2 where industry like 'Crypto%';
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';
select distinct country from layoffs_staging2 order by 1;
update layoffs_staging2 set country=trim(Trailing '.' from country) where country like 'United States%';
update layoffs_staging2 set `date`=str_to_date(`date`,'%m/%d/%Y');
alter table layoffs_staging2 MODIFY Column `date` date;
select t1.company,t1.industry,t2.company,t2.industry from layoffs_staging2 as t1 join layoffs_staging2 as t2 on t1.company=t2.company where (t1.industry is null or t1.industry='') and t2.industry is not null;
update layoffs_staging2 as t1 join layoffs_staging2 as t2 set t1.industry=t2.industry where t1.company=t2.company and t1.industry is null and t2.industry is not null;
update layoffs_staging2 set industry=null where industry='';
select * from layoffs_staging2 where total_laid_off ='' and percentage_laid_off ='';
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
select * from layoffs_staging2 where percentage_laid_off is null;
alter table layoffs_staging2 drop column row_num;