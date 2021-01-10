# Key concepts

## CAP Theorem:

Consistency: Every read from the database gets the latest (and correct) piece of data or an error

Availability: Every request is received and a response is given -- without a guarantee that the data is the latest update

Partition Tolerance: The system continues to work regardless of losing network connectivity between nodes

Is Eventual Consistency the opposite of what is promised by SQL database per the ACID principle?
Much has been written about how Consistency is interpreted in the ACID principle and the CAP theorem. Consistency in the ACID principle refers to the requirement that only transactions that abide by constraints and database rules are written into the database, otherwise the database keeps previous state. In other words, the data should be correct across all rows and tables. However, consistency in the CAP theorem refers to every read from the database getting the latest piece of data or an error.

Does Cassandra meet just Availability and Partition Tolerance in the CAP theorem?
According to the CAP theorem, a database can actually only guarantee two out of the three in CAP. So supporting Availability and Partition Tolerance makes sense, since Availability and Partition Tolerance are the biggest requirements.

If Apache Cassandra is not built for consistency, won't the analytics pipeline break?
If I am trying to do analysis, such as determining a trend over time, e.g., how many friends does John have on Twitter, and if you have one less person counted because of "eventual consistency" (the data may not be up-to-date in all locations), that's OK. In theory, that can be an issue but only if you are not constantly updating. If the pipeline pulls data from one node and it has not been updated, then you won't get it. Remember, in Apache Cassandra it is about Eventual Consistency.

# References

- https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/a7801de4-ee3f-4531-b887-82dea67f47a6/lessons/73fd6e35-3319-4520-94b5-9651437235d7/concepts/49e2905b-1cb3-4deb-b4c1-8f0c4da156c5
- https://en.wikipedia.org/wiki/CAP_theorem
- https://www.voltdb.com/blog/2015/10/disambiguating-acid-cap/