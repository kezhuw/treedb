# TreeDB
TreeDB is a cache path sensitive hierarchical data store, used as NoSQL database.

**TreeDB is in early stage of development, none parts should be considered as stable. Use it with your own caution!**

## Why another database ?
Database is a facility that provides both memory cache and disk storage, I concludes to this after
years development in game server scenarios. What hit me often is memory cache.

The primary reason to use memory cache is performance. Today, performance promotion benefits from
scenario insensitive cache used in disk oriented databases is far behind requirement. Memory cache
systems are adopted to alleviate database load. Hence boilerplates existed in many projects to handle
the similar situations: cache missings, cache loadings, cache consistence, and data writebacks.
Full memory databases, such as Redis, suffers from other problems: costs and capacities.

Things complicates things. The origin problem is that cache policy used by disk oriented databases
leads to little performance promotion. Why not give some hits to cache facility? This leads me to
create TreeDB.

## First look
Data stored in TreeDB are structed as hierachical tree structure. Data are accessed through path, similar
to the filesystem one. Here is the demonstration using `treedb` Go package.

```go
// Open database, if missing, clone it from database "template0".
db, err := treedb.Open("tcp://:3456", "test", &treedb.Options{TemplateDB: "template0", CreateIfMissing: true})

// Set cache pattern for all keys under tree "/game/users/".
// You can set cache patterns in template database.
db.Cache("/game/users/*", time.Minute * 30)

// Match previous cache pattern, the path "/game/users/10023" will be cached in memory
// as a whole for about 30 minutes.
userData, err := db.Get("/game/users/10023", treedb.FieldTree)

// No cache pattern matched, the path "/game/modules/arena" may not cached in memory based on other configs.
db.Get("/game/modules/arena", treedb.FieldBinary)

// Modify data.
db.Set("/game/users/10023/items/20003", binaryData)
db.Delete("/game/users/100034/items/20345")

// Update path's last access timestamp if in memory.
db.Touch("/game/users/10023")

// Delete the whole path, whether it is binary or tree.
db.Delete("/game/users/10023")

// Create whole tree at path.
var userData map[string]interface{}
db.Set("/game/users/10234", userData)
```

## Repository layout
The layout of this repository is divided to four parts:
- Server daemon `treedbd` locates in [cmd/treedbd](cmd/treedbd) directory.
- Command line tool `treedb` locates in [cmd/treedb](cmd/treedb) directory.
- Client-Server protocol locates in [protocol](protocol) directory, demonstrated as Go package.
- Go package as client interface to server is located in the top directory.

## TODO
- [ ] Customized leveldb to collect orphan tree in merging time
- [ ] Replication
    - [ ] Customized leveldb to store WAL for synchronization

## License
The MIT License (MIT). See [LICENSE](LICENSE) for the full license text.

## Contribution
Issues and pull requests are welcome.
