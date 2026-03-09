# Lending Club Loan Analytics (dbt + BigQuery + Power BI)

This project analyzes Lending Club loan data using the modern data stack.
The goal is to transform raw loan data using dbt and visualize key insights with Power BI.

## Project Architecture

Raw Data → BigQuery → dbt Transformations → Analytics Mart Tables → Power BI Dashboard

## Tools Used

* SQL
* dbt
* BigQuery
* Power BI
* GitHub

## dbt Models

The project includes multiple dbt layers:

* **Staging models** – cleaning and preparing raw Lending Club data
* **Intermediate models** – transformations and joins
* **Mart models** – analytics-ready tables used in dashboards

## Dashboard Insights

The Power BI dashboard explores:

* Loan approval vs rejection trends
* Loan amount distribution
* Borrower risk patterns
* Loan status analysis

## Repository Structure

analyses/ – analytical SQL queries
macros/ – reusable dbt macros
models/ – staging and mart models
seeds/ – seed data
snapshots/ – snapshot tracking
tests/ – dbt tests

## Future Improvements

* Add more dbt tests
* Add loan risk segmentation model
* Improve dashboard insights

## Dashboard Preview

![Dashboard](images/dashboard_preview.png)
