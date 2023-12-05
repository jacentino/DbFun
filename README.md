# DbFun 
![Latest build](https://github.com/jacentino/DbFun/actions/workflows/build-and-test.yml/badge.svg)

This project is successor of [SqlFun](https://github.com/jacentino/SqlFun).

The differences are explained in the project [wiki](https://github.com/jacentino/DbFun/wiki/Differences-between-DbFun-and-SqlFun).

## Features
* All SQL features available
* Type safety
* High performance
* Compound, hierarchical query parameters
* Compound, hierarchical query results
* Support for parameter conversions
* Support for result transformations
* Support for enum types
* Asynchronous queries
* Template-based queries
* Computation expressions for connection and transaction handling

## How it works
Most of us think about data access code as a separate layer. We don't like to spread SQL queries across all the application.
Better way is to build an API exposing your database, consisting of structures representing database data, and functions responsible for processing this data. 
DbFun makes it a design requirement.

### Configuration
First step is to define function creating database connection and config record:
```fsharp
let createConnection () = new SqlConnection(<your database connection string>)
let defaultConfig = QueryConfig.Default(createConnection)
```
and wire it up creating object responsible for generating queries:
```fsharp 
let query = QueryBuilder(defaultConfig)
```
and definiing function for executing them:
```fsharp 
let run dbCall = DbCall.Run(createConnection, dbCall)
```    
### Data structures
Then, data structures should be defined for results of your queries.
```fsharp 
type Post = {
    id: int
    blogId: int
    name: string
    title: string
    content: string
    author: string
    createdAt: DateTime
    modifiedAt: DateTime option
    modifiedBy: string option
    status: PostStatus
}
    
type Blog = {
    id: int
    name: string
    title: string
    description: string
    owner: string
    createdAt: DateTime
    modifiedAt: DateTime option
    modifiedBy: string option
    posts: Post list
}
```    
The most preferrable way is to use F# record types. Record fields should reflect query result columns, because they are mapped by name.
    
### Defining queries
The best way of defining queries is to create variables for them and place in some module:
```fsharp 
module Blogging =    
 
    let getBlog = query.Sql<int, Blog>(
        "select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy
         from Blog
         where id = @id",
        "id")
            
    let getPosts = query.Sql<int, Post list>(
        "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status 
         from post 
         where blogId = @blogId",
        "blogId") 
```        
The functions executing queries are generated during a first access to the module contents. 

At that stage, all the type checking is performed, so it's easy to make type checking part of automatic testing - one line of code for each module is needed.

The generating process uses little bit of reflection, but no reflection is used while processing a query, since generated code is executed.

### Executing queries
Queries defined above return `DbCall<'t>`, that is function taking IConnector object and returning data wrapped in Async. They can be passed to the `run` function after applying preceding parameters.
```fsharp
async {
    let! blog = Blogging.getBlog 1 |> run
}
```
The run function manages a database connection, that is opened and closed inside it. To execute more queries on one open connection the `dbsession` computation expression can be used:
```fsharp
dbsession {
    let! postId = Blogging.insertPost post
    do! Blogging.insertComments postId comments
    do! Blogging.insertTags postId tags
}
|> run
|> Async.RunSynchronously // because run returns Async
```
### Compound parameters
Records can be parameters as well::
```fsharp
let insertPost = query.Sql<Post, int>(
    "insert into post 
            (blogId, name, title, content, author, createdAt, status)
     values (@blogId, @name, @title, @content, @author, @createdAt, @status);
     select scope_identity()")
```
### Explicit parameter and result definition
There is an alternative way of defining parameters and results by using specifiers. It's more verbose, that method presented above:
```fsharp
let insertPost = query.Sql(
    "insert into post 
            (blogId, name, title, content, author, createdAt, status)
     values (@blogId, @name, @title, @content, @author, @createdAt, @status);
     select scope_identity()",
    Params.Record<Post>(),
    Results.Int "")
```
but gives the user lot of flexibility, e.g. provide parameter names representing tuple items:
```fsharp
let insertTag = query.Sql(
    "insert into tag (postId, name) values (@postId, @name)",
    Params.Tuple<int, string>("postId", "name"),
    Results.Unit)
```

### Result transformations
ADO.NET commands allow to specify queries returning multiple results. DbFun leverages it by providing special types of result specifiers, that combine subsequent results,
either for single master records with details:
```fsharp
let getOnePostWithTagsAndComments = query.Sql<int, Post>(
    "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status
     from post
     where id = @postId;
     select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt
     from comment c
     where c.postId = @postId
     select t.postId, t.name
     from tag t
     where t.postId = @postId",
    "postId",
    Results.Combine(fun post comments tags -> { post with comments = comments; tags = tags })
    <*> Results.Single<Post>()
    <*> Results.List<Comment>()
    <*> Results.List<string> "name")
```
or for collections of master and details records, matched by key:
```fsharp
let getManyPostsWithTagsAndComments = query.Sql<int, Post seq>(
    "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status
     from post
     where blogId = @blogId;
     select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt
     from comment c join post p on c.postId = p.id
     where p.blogId = @blogId
     select t.postId, t.name
     from tag t join post p on t.postId = p.id
     where p.blogId = @blogId",
    "blogId", 
    Results.PKeyed<int, Post> "id"
    |> Results.Join (fun (post, comments) -> { post with comments = comments }) (Results.FKeyed "postId")
    |> Results.Join (fun (post, tags) -> { post with tags = tags }) (Results.FKeyed("postId", "name"))
    |> Results.Unkeyed)
```
The code updating master record with detail values can be simplified thanks to quoting and ReflectedDefinition attribute:
```fsharp
let p = any<Post>
let getManyPostsWithTagsAndComments = query.Sql<int, Post seq>(
    "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status
     from post
     where blogId = @blogId;
     select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt
     from comment c join post p on c.postId = p.id
     where p.blogId = @blogId
     select t.postId, t.name
     from tag t join post p on t.postId = p.id
     where p.blogId = @blogId",
    "blogId", 
    Results.PKeyed<int, Post> "id"
    |> Results.Join p.comments (Results.FKeyed "postId")
    |> Results.Join p.tags (Results.FKeyed("postId", "name"))
    |> Results.Unkeyed)
```


