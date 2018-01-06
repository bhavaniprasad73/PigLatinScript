
/* Pig Latin Script for Word Count program for a given text file (eBook of Pride and Prejudice) */

Line = LOAD 's3://piglatinscript/Input/exercise1.txt' AS (line: chararray);            /* Loading data from hdfs into a pig relation */
Words = FOREACH Line GENERATE FLATTEN(TOKENIZE(line, ' ')) AS word;                    /* Foreach statement that will convert the sentence into words in multiple tuples */
Grouped = GROUP Words BY word;                                                         /* Grouping the words in a Words relation */
Wordcount = FOREACH Grouped GENERATE group, COUNT(Words);                              /* Foreach statement that will calculate the number of occurence of each word in given input file */
STORE Wordcount INTO 's3://piglatinscript/Output/FinalResult';                         /* This will store the computed result (key, value) pairs in a text file */


