# Lab 03: Airflow

## Assignment
In this lab, we'll schedule the pipeline with Airflow.

Create an Airflow DAG that:

- Starts on November 1 2023
- Runs daily
- Does the following:
    - Retrieve current data from [Austin Animal Shelter Outcomes](https://data.austintexas.gov/Health-and-Community-Services/Austin-Animal-Center-Outcomes/9t4d-g238)
            - You can use the Export -> CSV option
            - +5 points for API
            - You cannot use shelter1000.csv for this lab
        - Save the data to cloud storage
            - Use local storage for 80% of the credit (80% reduction applied only to this part)
            - If using local storage, it has to be accessible by other tasks
        - Transform the data into the dimensional data model
        - Save the transformed data to cloud storage
            - Use local storage for 80% of the credit (80% reduction applied only to this part)
            - If using local storage, it has to be accessible by other tasks
        - Loads the data into a cloud data warehouse
            - Use local postgres for 80% of the credit (80% reduction applied only to this part)
            - Loading of a single table should be its own task
            - The process should make sure that duplicate information isn't added

Things to consider:

- Retrieve the entire file or just the data for the necessary date?
- Drop all the existing data and rewrite it, or append/update existing records?
- Unless explicitly specified, not every step has to be its own task. How to break up the tasks?

Take a look at the walkthrough videos posted in Week 11 . They should get you most (but not all) of the way there.

You are encouraged to start with the code you have for Lab 02. Alternatively, you can use the setup provided in https://github.com/alexyarosh/csci5352-f23/tree/main/Lab-03/Setup for 90\% of the credit



 

Submission

    Check in the DAG file and any other files you used in the Lab into Github
    Open a PR
    Submit the link to the PR
