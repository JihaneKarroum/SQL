# Data Cleaning : 
USE world_layoffs;

SELECT * FROM layoffs;

# 1. Remove Duplicates
# 2. Standardize the Data
# 3. Null Values or blank values
# 4. Remove Any Columns 

# Garder raw data puisqu'on va faire des modifications de données :
CREATE TABLE layoffs_staging LIKE layoffs;

SELECT * FROM layoffs_staging;

insert layoffs_staging
SELECT * FROM layoffs;

# 1. Remove Duplicates :
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
delete FROM duplicate_cte
WHERE row_num > 1;

# 1. Verify Duplicates :
SELECT * FROM layoffs_staging where company='Casper';

CREATE TABLE layoffs_staging2 LIKE layoffs_staging;
SELECT * FROM layoffs_staging2;
ALTER TABLE layoffs_staging2 ADD COLUMN row_num INT;

INSERT INTO layoffs_staging2 
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

delete FROM layoffs_staging2 WHERE row_num > 1;

SELECT * FROM layoffs_staging2 WHERE row_num > 1;

# 2. Standardizing data : enlever les espaces blancs au début et les noms redondants ressemblables
SELECT company, TRIM(company) FROM layoffs_staging2;

UPDATE layoffs_staging2 set company = TRIM(company);

SELECT DISTINCT(industry) FROM layoffs_staging2 ORDER BY 1;
SELECT * FROM layoffs_staging2 WHERE industry LIKE 'Crypto%';

update layoffs_staging2 set industry='Crypto' WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country) FROM layoffs_staging2 ORDER BY 1;
update layoffs_staging2 set country=TRIM(TRAILING '.' FROM country) WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(country)  FROM layoffs_staging2 ORDER BY 1;

# Conversion de la date de TEXT à Date :
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')  FROM layoffs_staging2;
UPDATE layoffs_staging2 set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs_staging2;
alter table layoffs_staging2 modify column `date` DATE;

# 3. NULLs & Blank Values :
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL	;

SELECT * FROM layoffs_staging2 
WHERE industry IS NULL
OR industry = '';

SELECT * FROM layoffs_staging2 
WHERE company = 'Airbnb';

SELECT st.industry, st2.industry FROM layoffs_staging2 st
JOIN layoffs_staging2 st2
ON st.company = st2.company
WHERE (st.industry IS NULL OR st.industry = '')
AND st2.industry IS NOT NULL; 

UPDATE layoffs_staging2
SET industry = NULL WHERE industry = '';

UPDATE layoffs_staging2 st
JOIN layoffs_staging2 st2
ON st.company = st2.company
SET st.industry = st2.industry
WHERE (st.industry IS NULL OR st.industry = '')
AND st2.industry IS NOT NULL; 

# 4. Remove columns that we don't need : 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

Alter table layoffs_staging2
DROP COLUMN row_num;