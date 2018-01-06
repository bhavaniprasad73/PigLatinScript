input1 = LOAD 's3://piglatinscript/Input/exercise2.csv' USING PigStorage(',') AS (Year:int, Month:int, DayofMonth:int, DayofWeek:int, DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int, UniqueCarrier:chararray, FlightNum:int, TailNum:int, ActualElapsedTime:int, CRSElapsedTime:int, AirTime:int, ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray, Distance:int, TaxiIn:int, TaxiOut:int, Cancelled:int, CancellationCode:chararray, Diverted:int, CarrierDelay:int, WeatherDelay:int, NASDelay:int, SecurityDelay:int, LateAircraftDelay:int);   /* loading the input file from hdfs to pig relation */

on-time = FILTER input1 BY DepDelay < 15 AND ArrDelay < 15;                         /* Finding the on-time flights that have departure delay and arrival delay both < 15 minutes */


C = FOREACH input1 GENERATE UniqueCarrier, FlightNum;                                /* This projects the two columns UniqueCarrier and FlightNum from A relation */ 
D = FOREACH on-time GENERATE UniqueCarrier, FlightNum;                               /* This projects the two columns UniqueCarrier and FlightNum from A relation */

D1 = GROUP C ALL;                                                                    /* Group All operator */

D2 = GROUP D ALL;                                                                    /* Group All operator */ 


E = FOREACH D1 GENERATE 'Same' AS key, (DOUBLE) COUNT(C) AS total;                  /* Generates the total number of flights */
F = FOREACH D2 GENERATE 'Same' AS key, (DOUBLE) COUNT(D) AS some;                   /* Generates the number of on-time flights */


G = JOIN E BY key, F BY key;                                                         /* Joins two relations by key*/
DUMP G;                                                                              /* This displays the relation on the console */

H = FOREACH G GENERATE (F::some/E::total)*100;                                       /* This will generates the portion or percentage of the flights that are on-time */
DUMP H;                                                                              /* This will display the relation on the console */ 

STORE H INTO 's3://piglatinscript/Output/PortionResult';                             /* The results are stored in the output folder in the S3 bucket in the file name as PortionResult */ 
