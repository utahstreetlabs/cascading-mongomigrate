cascading-mongomigrate
===================

cascading-mongomigrate makes it easy to run Cascading flows on MongoDB
collections. It currently employs a naive and mostly broken splitting
strategy of partitioning ObjectIds from the collection into equal
ranges. Because ObjectIds are UUIDS, this will result in chunks of
different and arbitrary size. Fortunately, because a timestamp is used
for the most significant bits of the ObjectId, this will frequently be
a not-completely-insane thing to do. We use it at Copious to migrate data
from our MongoDB instances to HDFS.

Cascading-MongoMigrate is available [on Clojars](http://clojars.org/cascading-mongomigrate).

Usage
-----

To read data from a database in a Cascading flow, use DBMigrateTap.
DBMigrateTap's constructor has the following signature:

    MongoMigrateTap(
      int numChunks,          // The number of splits to create of the database.
                              // This will correspond to the number of mappers
                              // created to read the database.
      String host,            // Mongo host
      int port,               // Mongo port
      String username,        // Username to connect to your database.
      String pwd,             // Password to connect to your database.
      String collectionName,  // The collection to read during the flow.
      String pkColumn,        // The name of the ObjectId column of the table.
      String[] fieldNames,    // The names of the columns to read into the flow.
      Options options         // Optional, unused for now
    )

The tap will emit tuples containing one value for each field read, the value
names being the field names. Note that there is currently no special handling for
embedded documents - these values will be read into the flow as arrays or maps.
Newer versions of Cascalog support serializing these arrays and maps out of the
box.

Examples
--------

### Cascalog

```clojure
 (defn mongo []
   (MongoMigrateTap.
    4
    "127.0.0.1"
    27017
    "hungryman"
    "gimmesome"
    "breakfast_db"
    "hams"
    "_id"
    (str-arr ["weight" "color" "salt_levels"])
    (cascading.mongomigrate.tap.MongoMigrateTap$Options.)))

```

Gotchas
-------

If you're using cascalog, be sure to use `(:distinct false)` in any query that uses
embedded arrays directly, since cascalog can't necessarily compare seqs with eachother: from @sritchie:

    vectors actually are comparable inside of Fields, but Seqs aren't (necessarily). So something like this will fail:

    (defn bundle [& args]
        [args])

    (?<- (stdout)
           [?coll]
           (src ?a ?b ?c)
           (bundle ?a ?b ?c :> ?coll))

    While this will succeed:

    (defn bundle [& args]
        [(vec args)])

    (?<- (stdout)
           [?coll]
           (src ?a ?b ?c)
           (bundle ?a ?b ?c :> ?coll))


Building
--------

To build cascading-mongomigrate, run:

    lein jar

This will produce a single jar called `cascading-mongomigrate-<version>.jar`


TODO
----

A much better splitting strategy would involve introspecting a collection's
sharding strategy using it to inform the splits:

- discover/connect to slave replica sets automatically
- discover sharding strategy automatically and use it to create splits



Thanks to https://github.com/cascading/cascading-dbmigrate for providing the starting point for this library.

