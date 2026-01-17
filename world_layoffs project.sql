-- Data cleaning project 

select * from layoffs;

-- Remove any duplicates 
-- Standardiaze the data 
-- Check for any null columns/ values 
-- remove blank columns 

CREATE TABLE layoffs_staging 
like layoffs;

INSERT INTO layoffs_staging 
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS null
AND percentage_laid_off IS null;



SELECT*
FROM layoffs_staging; 
-- this table is created so that we do not messup any important data 

-- checking for duplicated data
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company,
                        industry,
                        total_laid_off,
                        percentage_laid_off,
                        `date`
       ) AS row_num
FROM layoff_staging;


SELECT *
FROM layoffs_staging; 

SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, 
                        industry,
                        total_laid_off,
                        percentage_laid_off,
                        stage, country, 
                        funds_raised_millions,
                        `date`
       ) AS row_num
FROM layoffs_staging;



WITH duplicate_cte AS 
(
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, 
                        industry,
                        total_laid_off,
                        percentage_laid_off,
                        stage, country, 
                        funds_raised_millions,
                        `date`
       ) AS row_num
FROM layoffs_staging
)
SELECT *
from duplicate_cte 
where row_num >1; 


SELECT * FROM
layoffs_staging
WHERE company = 'Yahoo';  


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

SELECT *
 FROM
layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, 
                        industry,
                        total_laid_off,layoffs_staging2
                        percentage_laid_off,
                        stage, country, 
                        funds_raised_millions,
                        `date`
       ) AS row_num
FROM layoffs_staging;



select * 
from layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;

DELETE 
from layoffs_staging2
WHERE row_num>1;

SELECT company, TRIM(company)
FROM layoffs_staging2; 

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1; 

SELECT *
FROM layoffs_staging2 
WHERE industry LIKE 'Crypto%' ; 


UPDATE layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%' ; 


SELECT *
FROM layoffs_staging2 
WHERE industry LIKE 'Crypto%' ;


SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1; 


SELECT *
FROM layoffs_staging2 
WHERE country LIKE 'United States%' ;


SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;  

SELECT DISTINCT country, trim(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1; 

UPDATE layoffs_staging2 
SET industry = trim(TRAILING '.' FROM country)
WHERE industry LIKE 'United States%' ;

SELECT *
FROM layoffs_staging2;

SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
fROM layoffs_staging2;

ALTER TABLE layoffs_staging2
modify COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2 
on t1.company = t2.company
WHERE (t1.industry is null or t1.industry = '')
AND t2.industry is not null; 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
on t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry is null or t1.industry = '')
AND t2.industry is not null; 
 
 DELETE 
 FROM layoffs_staging2 
 WHERE total_laid_off is null 
 AND percentage_laid_off is null; 
