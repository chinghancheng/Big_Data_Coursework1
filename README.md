# Big_Data_Coursework1
README
==============================================
* We have separated the task into three processes of job, each of them includes mapper step and previous two with reducer parser. 
* In Job1, the latest records of article is filtered based on time_stamp
  * MyRecordReader.java: Splite the data file(.txt) into records are needed in later process.
  * mapper.java: 
    > Filter the key and value we need. 
  
      (Key) article_title: page title 
      
        #set start=‘REVISION’, find the third space, move forward for one element.
        
      (Value) time_stamp: the exact date and time of the revision
      
        #set start=‘REVISION’, find the fifth space, move forward for one element.
        
      (Value) MAIN: title of the outside links
      
        #set start='MAIN', end='TALK', collect data between the start and the end.
      Key  | Values|
      --------- | --------|
      article_title  | time_stamp MAIN |
      ex.  Kelsey_Grammer  | 2007-02-18T03:32:53Z Matt_Lauer San_Diego Toy_Story_2 George_W._Bush... |
    
  * reducer.java:
    > Tranfer the time_stamp into ISO and then date format, and do the comparison for keeping the latest records of the articles then combine the values with key.
    * Apply the function in ISO 8601.java for transfering the time_stamp into ISO format.
    * Transter time_stamp from ISO to date format.
    * Keep latest records with the largest time_stamp and do the grouping by ','.
    * Set the initial value:1 for PageRank score.
    * Set '\t' for 
    > 
      Key  | Values|
      --------- | --------|
      article_title1  | time_stamp1 manin[i] |
      article_title1  | time_stamp2 manin[j] |
      article_title1  | time_stamp3 manin[k] |
    
    * if time_stamp2 is the latest record, the output would be:
    >
     Key  | Values|
     --------- | --------|
     article_title | PageRankScore time_stamp manin|
     article_title1  | 1 manin[j] |
     ex. article_title1  | 1 Matt_Lauer,San_Diego,Toy_Story_2,George_W._Bush... |
 * Generate the file iter00 as the result of job1.
 
* In Job2, 
  Input '!' 
     

      
      
      
