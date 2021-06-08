# Hacker News Demo Pipelines

This demo is set up as a "realistic" example of using dagster in a production world, integrating with
many of its features.

It consists of two pipelines:

- `download_pipeline`
  - This pipeline downloads events from the Hacker News API, splits them by type, and stores comments
    and stories into their own seperate tables in our Snowflake database.
- `story_recommender`
  - This pipeline reads from the tables that the `download_pipeline` writes to, and uses this data
    to train a machine learning model to recommend stories to specific users based on their comment history.

Here, you can find examples of using many of Dagster's features, including:

- Schedules / Sensors
- Asset Materializations
- IOManagers
- Modes / Resources
- Unit Tests
