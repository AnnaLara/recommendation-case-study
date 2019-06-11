from __future__ import division
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.cluster import KMeans
import pyspark


def ohe_columns(series, name):
    ohe = OneHotEncoder(categories='auto')
    ohe.fit(series)
    cols = ohe.get_feature_names(name)
    ohe = ohe.transform(series)
    final_df = pd.DataFrame(ohe.toarray(), columns=cols)
    return final_df

def add_clusters_to_users(n_clusters=8):
    """
    parameters:number of clusters
    
    return: user dataframe
    """
    
    # Get the user data
    user_df = pd.read_csv('data/users.dat', sep='::', header=None
                          , names=['id', 'gender', 'age', 'occupation', 'zip'])
    
    # OHE for clustering
    my_cols = ['gender', 'age', 'occupation']

    ohe_multi = OneHotEncoder(categories='auto')
    ohe_multi.fit(user_df[my_cols])
    ohe_mat = ohe_multi.transform(user_df[my_cols])

    # Then KMeans cluster
    k_clusters = KMeans(n_clusters=8, random_state=42)
    k_clusters.fit(ohe_mat)

    preds = k_clusters.predict(ohe_mat)
    
    # Add clusters to user df
    user_df['cluster'] = preds
    
    return user_df


def add_cluster_to_ratings(user_df):
    """
    given user_df with clusters, add clusters to ratings data
    parameters
    ---------
    user_df: df with user data

    returns
    -------
    ratings_df: ratings_df with cluster column
    """
    # Read in ratings file
    #Get ratings file - create Spark instance for loading JSON
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    ratings_df = spark.read.json('data/ratings.json').toPandas()
    
    # Set up clusters
    cluster_dict = {}
    for k, v in zip(user_df['id'].tolist(), user_df['cluster'].tolist()):
        cluster_dict[k] = v
        
    # Add cluster to ratings
    ratings_df['cluster'] = ratings_df['user_id'].apply(lambda x: cluster_dict[x])
    
    return ratings_df


def cluster_rating(df, movie_id, cluster):
    """Return the average rating for a movie, based on the cluster"""
    cluster_rating = df[(df['movie_id'] == movie_id) & (df['cluster'] == cluster)]
    return cluster_rating['rating'].mean()


def user_bias(df, user_id):
    """Calculate user bias: average rating for all users minus the users average rating"""
    return  df.loc[df['user_id'] == user_id, 'rating'].mean() - df['rating'].mean()


def item_bias(df, movie_id):
    """Calculate item bias: average rating for all items minus the items average rating"""
    return  df.loc[df['movie_id'] == movie_id, 'rating'].mean() - df['rating'].mean()


def get_cold_start_rating(user_id, movie_id):
    """
    Given user_id and movie_id, return a predicted rating
    
    parameters
    ----------
    user_id, movie_id
    
    returns
    -------
    movie rating (float)
    """
    # Load files 
    user_df = pd.read_csv('data/user_cluster.csv', index_col=0) 
    u_clusters = pd.read_csv('data/u_info.csv', index_col=0)
    ratings_df = pd.read_csv('data/movie_cluster_avg.csv', index_col=0)
    
    # User Cluster
    user_cluster = u_clusters.loc[u_clusters['id'] == user_id]['cluster'].tolist()[0]
    
    # Get score components
    if movie_id in ratings_df['movie_id'].tolist():
        avg = ratings_df.loc[(ratings_df['cluster'] == user_cluster) & (ratings_df['movie_id'] == movie_id)]['rating'].tolist()
    else:
        cluster_rating = ratings_df.loc[ratings_df['cluster'] == user_cluster]['rating'].tolist()
        avg = sum(cluster_rating)/len(cluster_rating)
        
    if user_id in user_df['user_id'].tolist():
        u = user_bias(user_df, user_id)
        i = item_bias(user_df, movie_id)
        pred_rating = avg + u + i
    else:
        pred_rating = avg
    
    return pred_rating