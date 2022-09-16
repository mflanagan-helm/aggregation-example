The Main.scala code demonstrates the aggregation step in our entity resolution pipeline.
This toy example has three record types

`CustomerProfile`
`VoterRecord`
`ResolvedProfile`

First, a set of `VoterRecord`'s are written and aggregated by `key`. Next, the `CustomerProfile`'s
are written and are joined against the aggregated `VoterRecord`s with a matching key. A 
`ResolvedProfile` is then generated to link a `CustomerProfile` with the best matching `VoterRecord`.
In this example the match is done by scanning the list of `VoterRecord`'s with the same key
as the `CustomerProfile` and finding the one with the nearest `value`.

Ideally, it would be nice to avoid the need to create the `voterRecordsByKeyTable` which will
end up with large messages (serialized `Map[Int, VoterRecord]` values). The problem with
large messages is discussed in detail in this [blog post on Foreign Key Joins](https://www.confluent.io/blog/data-enrichment-with-kafka-streams-foreign-key-joins/).

Unfortunately, I don't believe we can use the foreign key join capability here since the `key` value
is a many-to-many relationship between both datasets where the foreign key join requires one of the
tables to be joined on it's primary key in a one-to-many relationship.
