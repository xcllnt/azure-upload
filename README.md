# azure-upload

A simply python script that only uploads non-zero filled sectors
to Azure and doing so concurrently using multiple threads.
This minimizes the amount of data that needs to be uploaded on
the one hand and compensates for the increased latency on the
other hand.

Handy when bandwidth isn't great...
