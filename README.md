# KeyValueStore

This project was a motivation from Martin Kleppman's Designing Data Intensive Applications' 3rd chapter about Storage and Retrieval. The chapter initially talks about how Key Value stores are built using structured logs stored in files. It mentions about how a log can be maintained, scaled, indexed and later searched on using special data structures. Obviously, this was inspirational and thus I decided to replicate the design with some Go code. So here you go! I will be writing the key-value store, the indexing and merging mechanisms and the search APIs and ensure this system scales well while keeping good documentation and clean design in mind.


Design docs coming soon!

Some issues:
1. Map indexer compaction: how do you do the compaction? Copying and they compaction and then replace old with new? How will you copy consistently?
2. How do merge when the merging files can be in use? How do you update the file segment data to all these?
Sumukha PK.