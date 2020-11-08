Commoncrawl News Specific WARC File Parser
-
**Aim of the project** <br>
Extracts documents from commoncrawl news specific warc-files
<br>
<br>
 
**Requirements** <br>
-
 AWS  <br>
 EC2  <br>
 
 <br>
 
**How to run** <br>
-
 `python main.py --month_id 01 --year_id 2020 --month_half first`
 <br>
 <br>
 
**ToDo**
-
 - improve the overall warc file parsing workflow
    - the workflow should be more robust
 - remove parameters and it should parse in a parameterless fashion
    - maybe the month and year parameters are stored somewhere else
 - should be run in aws spot instances
 - it should have autoscaling so that weird instances are killed and new instances
 are spawned

 
 
 
 
 
 
 