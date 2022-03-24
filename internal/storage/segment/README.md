# Segment

This module, Segment is the lowest level of the storage system of the KeyValue Store.

## What does this module do?

Segment mimics a key value store in a much smaller scale as it has its own indexer in it. The idea of a segment is to split the huge data that the KeyValue store might have to take in, into smaller manageable segments so that the indexers or any other components like File APIs don't have to take the load.
    Append and Query to Segment thus involves a Key and the Value pair as 
the main arguments that the APIs revolve around. The `Segment` object can also be passed any IndexerGenerator to it and thus is agnostic of any indexer implementation as long as it behaves to the `indexer.Indexer` interface.

## API definition

Segment supports the following operations:
* Append - Enables appending to a segment. There are no limits for appending in terms of size enforced as such by the `Segment` module. Any limits that might exist will be from the underlying `os.File` module implementation in Go. Thus, reasonable limits must be set from the functions using the Segment API. This also finally indexes the data in its own indexer.
  
  `func (sg *Segment) Append(key string, data string) error`

* Query - Enables reading any appended data to the segment. Following the KeyValue logic, this needs the `Key` that was needed to `Append` the data. `Query` directly depends on the performance of the underlying indexer query operation, apart from that it's just a seeked file read.

  `func (sg *Segment) Query(key string) (string, error)`

* Print - this function is mostly for distress, debug or for devotion on the code you wrote to stare it in awe.

  `func (sg *Segment) Print()`