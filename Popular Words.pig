

/* Pig Latin Script for Word Count program for a given text file (eBook of Pride and Prejudice) */

Line = LOAD 's3://piglatinscript/Input/exercise1.txt' USING PigStorage(',') AS (line: chararray);            /* Loading data from hdfs into a pig relation */
Words = FOREACH Line GENERATE FLATTEN(TOKENIZE(line, ' ')) AS word;                    /* Foreach statement that will convert the sentence into words in multiple tuples */
Grouped = GROUP Words BY word;                                                         /* Grouping the words in a Words relation */
Wordcount = FOREACH Grouped GENERATE group, COUNT(Words) As occurence;                 /* Foreach statement that will calculate the number of occurence of each word in given input file */
STORE Wordcount INTO 's3://piglatinscript/Output/FinalResult';                         /* This will store the computed result (key, value) pairs in a text file */

/* Finding the 10 most popular words in the eBook of Pride and Prejudice */

Sort = ORDER Wordcount BY occurence DESC;                                              /* Sorting the tuples in a relation(Wordcount) by the occurence of word (occurence) in a Descending order */ 
Top = LIMIT Sort 10;                                                                   /* Limiting the relation to display only top 10 popular words by their count (occurences) */
STORE Top INTO 's3://piglatinscript/Output/TopResult';                                 /* Stores the top 10 popular words in a text file */

