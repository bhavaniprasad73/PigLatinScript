# PigLatinScript
Pig Latin Script (Hadoop & MapReduce)
Steps to execute the Pig Latin Script in Amazon Web Services (AWS) in the Batch Mode:

Here we have to use three Amazon services for executing the Script in AWS, for Storage (S3), Computing (EC2), Analytics (EMR).

1. Create a bucket in S3 (Simple Storage Service/ HDFS) (bucketname: "piglatinscript") and create four folders by name Input, Output, Script, Logs (for generating logs for execution history).

2. Upload the four input files (exercise1.txt, exercise2.csv, airports.csv, carriers.csv) in the Input folder in the S3 bucket.

3. Upload the Pig Latin Scripts in the scripts folder in the S3 bucket.

4. Create and configure the instances in the AWS Ec2 (Elastic Compute Cloud) that can be present in the 'Compute' section in the AWS management console under the Services. 

5. Create and configure the clusters in the AWS EMR (Elastic MapReduce) that can be present in the 'Analytics' section. 

6. To submit the jobs to the EMR clusters in the 'batch mode'.

7. Click on the cluster that select to run the jobs and go to the steps section and click on the Add step button.
    
   a) Select the Pig in the dropdown.
   b) Provide the path to the Scripts in the S3 bucket.
   c) Provide the path to the input file in the S3 bucket.
   d) Provide the path to the output folder in the S3 bucket.
   e) Leave the arguments box as is.
   f) Select the action on termination as 'continue' (as is). 
   g) click the button Submit.




Note : To view the outputs for the script, open the script's Output text file in the Notepad++ text editor for better view.
