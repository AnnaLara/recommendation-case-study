# Recommendation Systems: Cold Start Problem Case Study

## Assignment

Each user is potentially interested in watching one or more of the movies specified in `requests.json`. Our job is to decide which movie or movies to recommend to these users.

Use a combination of matrix factorization model (ALS) and a cold start model (using user and movie metadata) to fill in the NaN values and predict ratings for these movies.

Your predictions will be scored as follows:

1. Each user may watch movies from your list, starting with the highest predicted rating.
2. Your model will be scored based on how well the users liked the movies they watched.

Minimal requirements:

1. You must replace each NA value with a prediction.
2. You must create both a matrix factorization and a cold start model.
3. You team must document its work in a GitHub repo.
4. Your repo must include multiple commits from each team member.


Methodology:

Starting with users dataframe featuring demographic information (categories including age group, gender, occupation, and zip code), we dropped zipcode and one-hot encoded the other categories. Then we used k-means clustering to generate 8 demographic clusters.  Each user was then associated with a cluster; which we connected as dictionaries with user_id for key and cluster for value.
Using ratings dataframe (with user_id, movie_id, rating, and timestamp), we dropped the timestamp and added the cluster associated with the user_id. 
This enabled us to take a given movie id and find its mean rating for each cluster.
Finally, we built a user function that takes in user_id and movie_id which will check to see if the user has seen that movie. If not, it find that user's cluster and return the mean rating for that movie from other users in that cluster. 

