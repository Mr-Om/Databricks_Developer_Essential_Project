
# Data Ingestion Streaming Pipeline

## Implemented a multi-hop Delta Lake architecture using Spark Structured Streaming.

This architecture allows actionable insights to be derived from validated data in a data lake. Because Delta Lake provides ACID transactions and enforces schema, customers can build systems around reliable, available views of their data stored in economy cloud object stores.

### Scenario:

A video gaming company stores historical data in a data lake, which is growing exponentially.

The data isn't sorted in any particular way (actually, it's quite a mess) and it is proving to be very difficult to query and manage this data because there is so much of it.

Our goal was to create a Delta pipeline to work with this data. The final result is an aggregate view of the number of active users by week for company executives. To achieve the above we took the following steps:

- Created a streaming Bronze table by streaming from a source of files
- Created a streaming Silver table by enriching the Bronze table with static data
- Created a streaming Gold table by aggregating results into the count of weekly active users
- Visualize the results directly in the notebook
