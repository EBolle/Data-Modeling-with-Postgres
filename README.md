## What music are our users listening to? 

<img src="https://images.pexels.com/photos/3756943/pexels-photo-3756943.jpeg?auto=compress&cs=tinysrgb&dpr=3&h=750&w=1260" height="958" width="640">

Source: https://www.pexels.com

This very important question could not be answered, until now. As of today, the music database is ready to use!

### Insights help us grow

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

With the new database, our analytics team has all the data they need to answer these important questions and more.

### The schema

<img src="https://user-images.githubusercontent.com/49920622/103029645-85da9500-455a-11eb-8191-14369838eea6.png">

### Example queries

- number of unique users and songs played per year

```sql
SELECT year
,    count(*) as n_songs_played
,    count(distinct user_id) as n_distinct_users

FROM
    songplays
    inner join time on time.start_time=songplays.start_time
    
GROUP BY
    year
```

### Instructions

Before you can run the notebook and scripts, there are 3 things you need to do:
- create and activate a virtual environment
- create a .env file and set your credentials

#### create and activate a virtual environment 

You can either use Anaconda or venv to create the virtual environment. Regardless of your choice, you have to open
a (Anaconda) prompt, clone the project, and navigate to the project folder. Next, enter the following:

##### Anaconda
```bash
conda env create -f environment.yml
conda activate postgres
```

##### venv
```bash
python -m venv venv
venv\Scripts\Activate
```

#### create a .env file and set your credentials

Since this project needs to connect to a database, we need to store our credentials in a safe manner. To keep things
simple you can update the .env.example file, and remove .example from the filename. For more information about working
with secrets look [here](https://pybit.es/persistent-environment-variables.html).

You are now ready to use the project, it is recommended to start by looking at notebooks/main.ipynb.

### Contact

In case of suggestions or remarks please contact the Data Engineering department.