## What music are our users listening to? 

<img src="https://images.pexels.com/photos/3756943/pexels-photo-3756943.jpeg?auto=compress&cs=tinysrgb&dpr=3&h=750&w=1260">

Source: https://www.pexels.com

This very important question could not be answered, until now. As of today, the music database is ready to use!

### Insights help us grow

In the current music streaming industry, personalized content and recommendations are key for continuous growth. 
Competition is fierce and improving every day, so our customers expect nothing less from us. In order to do that,
our analytics team needs to be able to answer the following questions:
- what music do we expect our users have an interest in, besides the music they are already listening?
- how is our product used? e.g.,
    - what is the average session length of a typical user?
    - are there seasonal differences in usage?
    - is the listening behaviour different during the day compared to the evening?
- what artists do our users like, and is that different in comparison to typical hit lists? If so, how can we leverage
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

### Entry point

Please start in the notebooks/main.ipynb for the best experience.

### Contact

In case of any questions or comments please contact me directly via ernst@sparkify.com.