CREATE KEYSPACE beam
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
        };
CREATE TABLE beam.user_stat (
    userid text PRIMARY KEY,
    uniquesitescount bigint
);

CREATE TABLE beam.web_stat (baseUrl text,medianSessionLength double,count bigint, PRIMARY KEY (baseUrl));
