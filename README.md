# What music are our customers listening to?

In the current music streaming industry, personalized content and recommendations are key for continuous growth. 
Competition is fierce and improving every day, so our customers expect nothing less from us. In order to do that,
our analytics team needs to be able to answer the following questions:

- What music do we expect our users have an interest in, besides the music they are already listening to?
- How is our service used? 
    - What is the average session length of a typical user?
    - Are there seasonal differences in usage?
    - Is the listening behaviour different during the day compared to the evening?
- What artists do our users like, and is that different in comparison to typical hit lists? If so, how can we leverage
that?

With the new Postgres database, the analytics team has all the data they need to answer these important questions and
more.

## The schema

<img src="https://user-images.githubusercontent.com/49920622/103062485-97e62300-45ae-11eb-908d-4f27cca6f2a6.png">

## Example queries

- What are the differences in activity during the week? 

```sql
SELECT time.weekday
,    count(*) as n_songs_played
,    count(distinct sp.user_id) as n_unique_users
,    count(*) / count(distinct sp.user_id) as songs_per_user
,    count(*) / sum(count(*)) over () as perc_total_songs_played

FROM
    songplays as sp
    inner join time on time.start_time = sp.start_time
    
GROUP BY
    time.weekday
```

- Is there a difference in behaviour between paid and free users?

```sql
SELECT level
,    count(*) as n_songs_played
,    count(distinct user_id) as n_unique_users
,    count(*) / count(distinct user_id) as songs_per_user
,    count(*) / sum(count(*)) over () as perc_level

FROM
    songplays
    
GROUP BY
    level
```

- What is the gender distribution of our users, and are their differences in their activity?

```sql
SELECT users.gender
,    count(*) as n_songs_played
,    count(distinct sp.user_id) as n_unique_users
,    count(*) / sum(count(*)) over () as perc_songs_played

FROM
    songplays as sp
    inner join users on users.user_id = sp.user_id
    
GROUP BY
    users.gender
```

## Instructions

Before you can run the notebook and scripts, there are a few things you need to do:
- create and activate a virtual environment
- create a .env file and set your credentials

Furthermore, note that the logic needed to execute either the scripts or the notebook is stored in the /src folder.
The Python version used for this project is 3.8.5. 

### create and activate a virtual environment 

You can either use Anaconda or virtualenv to create the virtual environment, if you are not familiar with virtual
environments please read this [article][virtual_envs] first.

```bash
conda env create -f environment.yml
conda activate postgres

python -m venv venv
venv\Scripts\Activate
pip install -r requirements.txt 
```

### create a .env file and set your credentials

This project requires to work with secret (database) credentials. To keep things safe you can update the .env.example
file, and remove .example from the filename. For more information about working with secrets look [here][secrets]. 

## Start

Start by browsing main.ipynb and execute the scripts afterwards. 

```bash
cd Data-Modeling-with-Postgres

python scripts/create_tables.py
python scripts/etl.py
```

Note that the order of execution matters.

## Contact

In case of suggestions or remarks please contact the Data Engineering department.

[virtual_envs]: https://realpython.com/python-virtual-environments-a-primer/
[secrets]: https://pybit.es/persistent-environment-variables.html