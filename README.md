# KeyValueStore

This project was a motivation from Martin Kleppman's Designing Data Intensive Applications' 3rd chapter about Storage and Retrieval. The chapter initially talks about how Key Value stores are built using structured logs stored in files. It mentions about how a log can be maintained, scaled, indexed and later searched on using special data structures. Obviously, this was inspirational and thus I decided to replicate the design with some Go code. So here you go! I will be writing the key-value store, the indexing and merging mechanisms and the search APIs and ensure this system scales well while keeping good documentation and clean design in mind.


Design docs coming soon!

Some issues:
1. Map indexer compaction: how do you do the compaction? Copying and they compaction and then replace old with new? How will you copy consistently?
2. How do merge when the merging files can be in use? How do you update the file segment data to all these?
3. What basis are the merging happening??? They should be finding same keys and merge or something right? Whats happening now is basically putting data worth 5 files into one file. Which is complete bs. 
4. There was some idea I had about having multiple instances of KV store and then firing them with the queries and seeing if they have consistent results. This doesnt make sense now, but it did when I woke up from my sleep and seen this in my dream. Work on this, there may be something.

Sumukha PK.