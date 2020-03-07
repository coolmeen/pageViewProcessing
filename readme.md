
disclaimer: 
i don't have real expirience with stream/batch process in large scale, all the stuff i write here is based on articles a books i have read.



after reading the book Streaming Systems by  Tyler Akidau that describes the beam model, i thought this is a good way of practicing it.
so i choose apache beam the underlying data processing framework.
i know it's in overkill because we are dealing with low scale batch processing , but i wanted to get my hands on the api for long time and now i had a good reason.


beam is an abstraction over data processing frameworks(https://beam.apache.org/documentation/runners/capability-matrix/), 
they argue for a unified model for batch and stream processing and describe.
in this assigment i chose the beam's "direct-runner", but one can choose the runner it want's based on it's capabilites(spark,flink). 

**basic architecture**
1. read the csv files pattern from the commandline args and transform them to pageView object
2. run two beam pipelines for find all the unique visits per user and calculate session statistics per website
    1. the unique visits pipeline is pretty basic, group by userId, remove dups  store to cassandra
    2. session stats:
        1.  we start the sessions stats pipeline by grouping by user and website and putting this data to session window of 30 minutes
        2.  that we split the stream to two different stream, one calculating the length of each session windows, and other count it as 1
        3.  then we calculate the median of the session length, sum the num of windows.
        4.  join two streams together 
        5. write the data to cassandra
 
 **running instructions**:
 you can run mvn package and then run the jar with :
 1. vm options = -Dspring.profiles.active=main -Xmx6000
 2. arguments = filepattern(input_*.csv.
 
it runs with a tomcat web service and embedded cassandra.

the web-service starts on port 8080 and swagger on :

http://localhost:8080/swagger-ui.html#/
 
**time spent:**
1. 4-5 hours playing around apache beam api(https://beam.apache.org/documentation/programming-guide/#pardo) and understanding it's model.
2. 6 hours write the code/test



**things to notice**:  
if i had more time i would : 
1. understand why the direct runner has high memory consumption, maybe because of the shuffling for each Dofn function(groupBy) or checkpointing or maybe because of all the windowing and merging.
2. write some more tests, i didn't test all the pipelines steps only end-to-end.
3. create better model cassandra tables, this is not real solution, if we want data stored for long period we better preform inserts with timestamp of current date
 instead of updates.
 
 
**for more scalability:**
1. run it with spark/flink/any other runner in distributed mode should work without any major changes.
1. for lower latency and if we have continuous incoming data if needed we should consider switching the pipeline for streaming instead of batch, but than we would have to deal with watermarks and late arrivals
2. we can read the csv and store it kafka and distribute the data over the topic's partition to multiple beam runners(spark).
3. i am currently writing to cassandra directly, a better solution is maybe again use kafka as mid layer for data reliability.
4. i am inserting the data one by one, maybe there is a way to batching the data and insert all at once if we are working in a batch style

**Space and time complexity**
1. this is tricky because i used beam and i am not sure what is the overhead for that, but if are talking about pure running time:
    1. grouping each all the keys is simply creating some kind of map so that is o(n)
    2. calculating sessions is tricky, i am not sure how it works in beam but from my understanding there are alo of window merging processs, but if don't think about that
    calculating the window is simply o(1) so this o(n)
    3. then for each windows we need to find the length so again this o(n) in the worst case and then calculating the median is o(n)
    
    so basicly it's o(n) :)
2. memory calculation is above my pay grade, the best answer would be i don't know but the shuffling process is doubling and tripling the memory consumption also also the windowing is alot.
3. also we have concurrency in place here so all memory is tippled or even more because of immutability.

I wrote some end-to-end tests but not alot, also i tested your input with your output. 

 Also i got some different results in the output than what you presented.

 
