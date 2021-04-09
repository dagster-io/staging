# Hacker News Story Recommender Example

The Hacker News Story Recommender is a "realistic", maximalist example that's meant to demonstrate what it looks
like to use Dagster with all its bells and whistles.

It includes a pipeline that trains a machine learning model that recommends Hacker News stories
based on what stories a user has commented on in the past, then uses that model to make
recommendations.  It uses a public dataset of Hacker News stories and comments that is published to
GCS.
