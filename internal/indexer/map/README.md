# Map Indexer

Map indexer is a method to index the database objects which are held in files, in a map. The map has the key value as the key in it and the value is the offset data at which the value of this particular key is present.
This provides a major advatange than doing an entire sweep of the file contents and finding the key value.