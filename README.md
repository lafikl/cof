# COF
Column Oriented Files. This is a package i extracted from a bigger project that i stumbled upon when i was looking through `~/projects` directory on an old laptop that i had. The code was written in 2017. I want to see if adding genrics to it would make it better, but i'll leave that to a later time. I'm pushing it here just so i can refer to it when i need to.

The other parts which i didn't publish are:
- Query Execution Engine: it accepts a json object query that can do aggregate functions, filteration, group by.
- Web UI to interface with the query execution engine.

*Docs from 2017*


# Writing to a file
```go
urlCol := NewStringColumn()

schema := Table{
    "url": NewStringCol(),
    "serverIP": NewStringCol(),
    "clientIP": NewStringCol(),
    "timestamp": NewStringCol(),
}

store.New(schema)

bulk := []Record{}
record1 := Record{
    "url": "https://google.com",
    "serverIP": "94.0.0.1",
    "clientIP": "127.0.0.1",
    "timestamp": "1520000",
}
bulk = append(bulk, record1)
store.Batch(bulk)

```