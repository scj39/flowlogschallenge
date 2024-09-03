# Section 1: Analysis & Discussion

The problem asks us to process a potentially large (but well-structured) file, and, given a lookup table, produce two derivative files. 
For this problem, I took a MapReduce-based approach as a more generally scalable approach to the problem. Reading a large file is an I/O bound task that benefits from concurrent reads of the file, whereas processing the chunked reads during the map and reduce steps respectively is CPU-bound. 

Here, I read the file into memory. The ideal solution is to ingest a certain number of lines into memory for each coroutine, but this involves some potentially really hairy mappings between bytes ingested and line endings. Given that the flow log size does not exceed 10MB (well within what a process' memory can contain) I opted instead for the less scalable but more straightforward approach of reading synchronously. 

Afterwards, I chunk the file into portions. The portions then have a map operation performed on them. 

As there is a many-to-one relationship between the number of lines in a file and the frequency dictionary entries, I opt to concurrently process the map operations. I then process the reduce operation. While not necessary given the problem constraints, I could potentially also have chunked and concurrently processed the dictionaries used during the reduce operation. 

The results of the reduce operation are used to create the two derived files, which are then written back to the outputs folder. 

---

# Section 2: Assumptions & Callouts

I assume: 
* the flowlog files are in the default log format (version 2)
* while a tag can correspond to multiple destport-protocol combinations, a single destport-protocol combination cannot belong to two or more tags
* the tag counts add up to the total number of destport-protocol combination associated with that tag that are observed in the flow log
* the port-protocol count of matches refers only to the destport and not to the srcport. I.e., we are calculating destport-protocol totals 
* the lookup table provided is of the structure specified in the example
* untagged is not a unique tag of its own, but refers to all flow log entries that did not correspond to a matching tag in the lookup table

---

# Section 3: Instructions / Test Cases

Run ```python processflows.py -f insert-flowlog-path-here -l insert-lookuptable-path-here```

E.g., ```python processflows.py -f flowlogs/testflowlog -l lookuptables/testlookuptable.csv``` to run the test case provided. 

Outputs will be in the outputs folder -- see portprotocolcount.csv for table 2 and tag2count.csv for table 1

There's more documentation in processflows.py so please read it. 

For testing, I tried the example files provided, as well as cases where either the flow log or the lookup table was missing, as well as a scaled up flowlog of 2000 entries (roughly ~0.19 MB), which has been included in this submission -- see testflowlog and testlookuptable.csv respectively. 

Thanks for reading!
