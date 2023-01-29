select * from dbo.covid_deaths

-- select * from dbo.covid_vaccines

--Query:  Getting total_cases for each Continent

select continent,total_cases 
from 
(select  continent,total_cases, dense_rank() over(partition by continent order by total_cases desc) as rnk 
from dbo.covid_deaths where continent is not null) as x
where x.rnk <2

select continent,max(total_cases) as Total_Cases
from dbo.covid_deaths  where continent is not null
group by continent

-- Observations: North America has highest number of Covid Cases. 
----------------------------------------------------------------------------------------------------------------

select location,date,total_cases,new_cases,total_deaths,population
FROM dbo.covid_deaths
order by location,date desc


-- Query: Total cases vs Total Deaths for INDIA
with cte as (
select location,date,total_cases,total_deaths,concat(round((total_deaths/total_cases)*100,2),'%') as death_percentage
from dbo.covid_deaths
where location = 'India')
select * from cte
-- Observations: So we can see that in the end of April 2021,if the person got infected by covid.The chance that the person would die was around 1.11%

select location,date,total_cases,total_deaths,(concat(round((total_deaths/total_cases)*100,2),'%')) as death_percentage
from dbo.covid_deaths WHERE 
location = 'India' AND continent is not null
order by death_percentage desc
--Observations: The maximun death percentage was in the month of April,2020 where the total cases were 9205 and deaths were 331 


-- Query: Population VS Total Cases 
select location,date,population,total_cases,concat(round((total_cases/population)*100,3),'%') as Covid_Infection_Percentage
from dbo.covid_deaths
where continent is not null
AND location = 'India'
-- Observations : Total 1.39% percent of population got infected by covid at end of April,2021. The total cases we had at that time were around 1.9 Crores


-- Query : Comparing Countries population with the Covid Infection Rate
select location,population,Max(total_cases) as Highest_Cases, round(max((total_cases/population))*100,2) as Covid_Infection_Percentage
from dbo.covid_deaths
where continent is not null
Group by location,population
order by Covid_Infection_Percentage desc


--Query:  Countires with Highest Death Counts
select location,max(cast(total_deaths as int)) as Total_Deaths
from dbo.covid_deaths 
where continent is not null
group by location
order by Total_Deaths desc

--- Observations: so here we can see some inconsistent data. There are some continents in the location column so we need to remove them
-- and applying the same changes for above querries also

select location,max(cast(total_deaths as int)) as Total_Deaths
FROM dbo.covid_deaths 
Where continent IS NOT NULL
group by location
order by Total_deaths desc

--- Obsevations: The country with Highest number of Deaths was United States with 576k deaths in total. Followed by Brazil-403K, Mexico-216k, India - 211k, 
---               United Kingdom - 127k deaths respectively till the date April 2021.

-- Query : Total Deaths Per Continent
select continent, max(cast(total_deaths as int)) as Total_deaths
from dbo.covid_deaths
where continent is not null
group by continent
order by total_deaths desc
-- Observations : North America has the highest number of deaths 

--- Query:  Death Counts and Death cases Per Day Globally:

select date,sum(new_cases) as Total_Cases, sum(cast(new_deaths as int)) as Total_Deaths, sum(cast(new_deaths as bigint))/sum(new_cases)*100 as Death_Percentage
from dbo.covid_deaths
where continent is not null
group by date
order by date

-- Query: Global Death Count
select sum(new_cases) as Total_Cases,sum(cast(new_deaths as int)) AS Total_Deaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as Total_Deaths_Globally
from dbo.covid_deaths
where continent is not null
-- Observations : There were around 1.5 Billion cases Globally with total death count of 3.1 Million


--- Total Population vs Vaccinations for INDIA

select cd.location,cd.date,cd.population,cv.new_vaccinations as Vaccination_per_Day
from dbo.covid_deaths as cd JOIN dbo.covid_vaccines cv 
     ON cd.location = cv.location and cd.date  = cv.date
WHERE cd.continent is not null and  cd.location = 'India' 
-- Observations: Vaccinations started from 15 Jan,2021 in India and then the numbers grew rapdidly

select cd.location,cd.date,cd.population,cv.new_vaccinations as Vaccination_per_Day,
(max(cast(cv.new_vaccinations as int)) OVER()) as Highest_vaccinations 
from dbo.covid_deaths as cd JOIN dbo.covid_vaccines cv 
     ON cd.location = cv.location and cd.date  = cv.date
WHERE cd.continent is not null and cd.location = 'India' and cv.new_vaccinations = 4265157
 -- Observations: 2 April,2021 was the date when India had most number of people vaccinated in a single day



 -- Adding Column Cumulative sum for each location by Vaccination done on each date
 with cte as (
 select cd.continent,cd.location,cd.date,cd.population,cv.new_vaccinations AS Vaccination_per_Day,
 sum(convert(int,cv.new_vaccinations)) OVER(partition by cd.location order by cd.location,cd.date) as Cumulative_SUM
 from dbo.covid_deaths cd JOIN dbo.covid_vaccines cv 
      ON cd.location = cv.location and cd.date = cv.date 
WHERE  cd.continent IS NOT NULL )

select *,concat((Cumulative_Sum/population)*100,'%') as Percentage_cumulative 
from cte order by continent, location,date


-- New Table for storing above query
DROP Table IF EXISTS #PopulationVaccinationPercentage
create table #PopulationVaccinationPercentage
(Continent nvarchar(255),Location nvarchar(255),Date datetime,
Population int,Vaccination_Per_Day numeric,Vaccine_Cumulative_Sum numeric)

INSERT INTO #PopulationVaccinationPercentage
select cd.continent,cd.location,cd.date,cd.population,cv.new_vaccinations,
sum(convert(int,cv.new_vaccinations)) over(partition by cd.location order by cd.location,cd.date) as Cumulative_Sum
from dbo.covid_deaths cd JOIN dbo.covid_vaccines  cv
ON cd.location = cv.location and cd.date = cv.date
WHERE cd.continent IS NOT NULL

select *,(Vaccine_Cumulative_Sum/population)*100
from #PopulationVaccinationPercentage


-- Creating A View for our Client

create view PopulationPercentageVaccination AS
select cd.continent,cd.location,cd.date,cd.population,cv.new_vaccinations AS Vaccination_Per_Day,
sum(convert(int,cv.new_vaccinations)) OVER(partition by cd.location order by cd.location,cd.date) AS 
Vaccine_Cumulative_Sum
FROM dbo.covid_deaths cd JOIN dbo.covid_vaccines cv 
ON cd.location = cv.location and cd.date= cv.date
WHERE cd.continent is NOT NULL

select * from PopulationPercentageVaccination


-- Creating view for Total Cases and Deaths

create view Global_Details AS
select continent,sum(new_cases) as Total_Cases,sum(convert(int,new_deaths)) as Total_Deaths
from dbo.covid_deaths 
where continent is NOT NULL
group by continent

select * from global_details

 



----- Queries for Tableau Visualization

-- Total Death Percentage

select sum(new_cases) as Total_cases, sum(convert(int,new_deaths)) as Total_deaths , 
sum(convert(int,new_deaths))/sum(new_cases)*100 as Death_Percentage
from dbo.covid_deaths
where continent is NOT NULL
ORDER BY Total_cases, Total_deaths


 

-- 2 Getting  Infected Percentage of Population
select location,population,max(total_cases) as HighestInfected,max(total_cases/population)*100 as 
Infected_Population_Percentage
from dbo.covid_deaths 
where continent is not null
group by location,population
order by Infected_Population_Percentage DESC


--- 3 Continent wise Total Deaths
select continent ,sum(convert(int,new_deaths)) as Total_Deaths
from dbo.covid_deaths 
where continent is NOT NULL
group by continent
order by Total_Deaths desc


-- 4 Infected Percentage BY date
select location,population,date, max(total_cases) as Highest_Cases, max((total_cases/population))*100 as InfectionPercentage
from dbo.covid_deaths 
where continent is not null
group by location,population,date
order by InfectionPercentage desc




