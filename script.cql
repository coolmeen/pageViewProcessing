CREATE KEYSPACE beam WITH replication = {'class'': ''SimpleStrategy'', ''replication_factor': '1'} AND durable_writes = true;
CREATE TABLE beam.user_stat (
    userid text PRIMARY KEY,
    uniquesitescount bigint
);

CREATE TABLE web_stat (baseUrl text,medianSessionLength double,count bigint, PRIMARY KEY (baseUrl));
