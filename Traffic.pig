a = LOAD 's3://piglatinscript/Input/exercise2.csv' USING PigStorage(',') AS (year:int, Month:int, DayofMonth:int, DayofWeek:int, DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int, UniqueCarrier:chararray, FlightNum:int, TailNum:chararray, ActualElapsedTime:int, CRSElapsedTime:int, Airtime:int, ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray, Distance:int, TaxiIn:int, TaxiOut:int, Cancelled:Boolean, CancellationCode:chararray, Diverted:int, CarrierDelay:int, WeatherDelay:int, NASDelay:int, SecurityDelay:int, LateAircraftDelay:int);                                                                    /* loading the input file from the hdfs into pig relation */
b = LOAD 's3://piglatinscript/Input/airports.csv' USING PigStorage(',') AS (iata:chararray,airport:chararray,city:chararray,state:chararray,country:chararray,lat:double,long:double);  /* loading the another input file from the hdfs into pig relation */

a_mod = FOREACH a GENERATE FlightNum, Origin, Dest;                                        /* Projection of three columns from a relation and forming a new relation */
b_mod = FOREACH b GENERATE iata,airport,city;                                              /* Projection of three columns from a airports relation and forming a new relation */
b_fin = FOREACH b_mod GENERATE REPLACE (iata,'\\"', '') AS iata1, airport,city;            /* Replacing two characters with nothing */
a_group = GROUP a_mod BY Dest;                                                             /* Grouping a relation with destination */
inbound_count = FOREACH a_group GENERATE group,COUNT(a_mod) AS count1;                     /* Grouping and their count */
sorted = ORDER inbound_count BY count1 DESC;                                               /* Ordering the relation with count in descending order */
top_3 = LIMIT sorted 3;                                                                    /* Limiting to three first tuples */
DESCRIBE top_3;                                                                            /* Shows the schema for the relation top_3 */
DUMP top_3;                                                                                /* Displays the relation on the console */
joinab = JOIN top_3 BY group, b_fin BY iata1;                                              /* Joining top_3 relation by group with b_fin by iata1 */
DESCRIBE joinab;                                                                           /* Schema for the joinab relation */
re_sort = ORDER joinab BY count1 DESC;                                                     /* Orders the relation by count in descending order */
STORE re_sort INTO 's3://piglatinscript/Output/inbound';                                   /* Stores the results to the S3 Bucket in output folder with a filename of inbound */

