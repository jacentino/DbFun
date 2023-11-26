namespace DbFun.Core.Tests

open System
open Xunit
open DbFun.Core
open DbFun.Core.Builders
open DbFun.Core.Builders.MultipleResults
open DbFun.TestTools
open DbFun.TestTools.Models
open DbFun.TestTools.Mocks
open DbFun.Core.Diagnostics
open System.Data

module QueryTests = 

    type Criteria = 
        {
            name    : string option
            roles   : string list option
            statuses: Status list option
        }

    let createConfig createConnection = QueryConfig.Default(createConnection)

    [<Fact>]
    let ``Simple queries``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection)
               
        let query = qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")

        let connector = new Connector(createConnection(), null)        

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Fact>]
    let ``Simple queries - param by name``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql<int, User>("select * from User where userId = @Id", "id", Results.Auto()) 

        let connector = new Connector(createConnection(), null)        

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Fact>]
    let ``Simple queries - simplest form``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql<int, User>("select * from User where userId = @Id", "id") 

        let connector = new Connector(createConnection(), null)        

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Fact>]
    let ``Multiple results``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "userId"; col<string> "name";  ],
                    [
                        [ 1; "Administrator" ]
                        [ 1; "Guest" ]
                    ]                           
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql(
            "select * from User where userId = @id;
             select * from Role where userId = @id",
            Params.Auto<int>("id"), 
            Results.Multiple(Results.Single<User>(""), Results.Seq<string>("name")))            

        let connector = new Connector(createConnection(), null)        

        let user, roles = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)
        Assert.Equal<string seq>(["Administrator"; "Guest"], roles)


    [<Fact>]
    let ``Multiple results applicative``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "userId"; col<string> "name";  ],
                    [
                        [ 1; "Administrator" ]
                        [ 1; "Guest" ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql(
                "select * from User where userId = @id;
                 select * from Role where userId = @id",
                Params.Auto<int>("id"),
                Results.Combine(fun user roles -> user, roles)
                <*> Results.Single<User>("") 
                <*> Results.Seq<string>("name"))                

        let connector = new Connector(createConnection(), null)        

        let user, roles = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)
        Assert.Equal<string seq>(["Administrator"; "Guest"], roles)


    [<Fact>]
    let ``Joins with lambda merge``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "userId"; col<string> "name";  ],
                    [
                        [ 1; "Administrator" ]
                        [ 1; "Guest" ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql (
            "select * from User where userId = @id;
             select * from Role where userId = @id",
            Params.Auto<int>("id"),
            Results.Seq(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
            |> Results.Join (fun (u, rs) -> { u with UserWithRoles.roles = rs }) (Results.Seq(Rows.FKeyed<int, string>("userId", "name")))
            |> Results.Map (Seq.map snd))
                

        let connector = new Connector(createConnection(), null)        

        let user = query 1 connector |> Async.RunSynchronously |> Seq.toList

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
                roles = [ "Administrator"; "Guest" ]
            }

        Assert.Equal<UserWithRoles seq>([expected], user)
            

    [<Fact>]
    let ``Joins with expr merge``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "userId"; col<string> "name";  ],
                    [
                        [ 1; "Administrator" ]
                        [ 1; "Guest" ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection)
        let uwr = any<UserWithRoles>

        let query = 
             qb.Sql (
                "select * from User where userId = @id;
                 select * from Role where userId = @id",             
                Params.Auto<int>("id"),
                Results.Seq(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
                |> Results.Join uwr.roles (Results.Seq(Rows.FKeyed<int, string>("userId", "name")))
                |> Results.Unkeyed)
                

        let connector = new Connector(createConnection(), null)        

        let user = query 1 connector |> Async.RunSynchronously |> Seq.toList

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
                roles = [ "Administrator"; "Guest" ]
            }

        Assert.Equal<UserWithRoles seq>([expected], user)


    [<Fact>]
    let ``Grouping with lambda merge``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created"; col<string> "roleName" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1); "Administrator" ]
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1); "Guest" ]
                    ]
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql (
            "select u.*, r.name as roleName from User u left join Role r on u.userId = r.userId
             where u.userId = @id",
            Params.Auto<int>("id"),                        
            Results.List(Rows.Tuple<UserWithRoles, string option>("user", "roleName"))
            |> Results.Group (fun (u: UserWithRoles) rs -> { u with roles = rs }))

        let connector = new Connector(createConnection(), null)        

        let user = query 1 connector |> Async.RunSynchronously 

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
                roles = [ "Administrator"; "Guest" ]
            }

        Assert.Equal<UserWithRoles list>([expected], user)


    [<Fact>]
    let ``Grouping with expr merge``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created"; col<string> "roleName" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1); "Administrator" ]
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1); "Guest" ]
                    ]
                ]

        let qb = QueryBuilder (createConfig createConnection)

        let query = qb.Sql (
            "select u.*, r.name as roleName from User u left join Role r on u.userId = r.userId
             where u.userId = @id",
            Params.Auto<int>("id"),                        
            Results.List(Rows.Tuple<UserWithRoles, string option>("user", "roleName"))
            |> Results.Group any<UserWithRoles>.roles)

        let connector = new Connector(createConnection(), null)        

        let user = query 1 connector |> Async.RunSynchronously 

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
                roles = [ "Administrator"; "Guest" ]
            }

        Assert.Equal<UserWithRoles list>([expected], user)

            
    [<Fact>]
    let ``Procedures``() = 

        let createConnection() = 
            setupCommandOutParams
                [   "userId", box 1
                    "name", box "jacentino"
                    "email", box "jacentino@gmail.com"
                    "created", box (DateTime(2023, 1, 1))
                ]

        let qb = QueryBuilder (createConfig createConnection)
               
        let query = qb.Proc(Params.Auto<int> "id", OutParams.Record<User>(), Results.Unit) "getUser"

        let connector = new Connector(createConnection(), null)        

        let _, user = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)

            
    [<Fact>]
    let ``Procedures - simplest form``() = 

        let createConnection() = 
            setupCommandOutParams
                [   "userId", box 1
                    "name", box "jacentino"
                    "email", box "jacentino@gmail.com"
                    "created", box (DateTime(2023, 1, 1))
                ]

        let qb = QueryBuilder (createConfig createConnection)
               
        let query = qb.Proc<int, User, unit>("id") "getUser"

        let connector = new Connector(createConnection(), null)        

        let _, user = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)

    [<Fact>]
    let ``Compile-time errors - immediately``() = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                            
                ]

        let qb = QueryBuilder (createConfig createConnection)
               
        let ex = 
            Assert.Throws<CompileTimeException>(fun () -> qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "") |> ignore)
        Assert.Contains("QueryTests.fs", ex.Message)
        Assert.Contains("line: 374", ex.Message)


    [<Fact>]
    let ``Compile-time errors - logging``() = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                            
                ]

        let qb = QueryBuilder(createConfig createConnection).LogCompileTimeErrors()
               
        qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")               
        |> ignore

        let line, file, _ = qb.CompileTimeErrorLog |> List.head

        Assert.Equal(395, line)
        Assert.Contains("QueryTests.fs", file)


    [<Fact>]
    let ``Compile-time errors - deferred throw``() = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                            
                ]

        let qb = QueryBuilder(createConfig createConnection).LogCompileTimeErrors()
               
        let query = qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")                   

        let connector = new Connector(createConnection(), null)        

        let ex = Assert.Throws<AggregateException>(fun () -> query  1 connector |> Async.RunSynchronously |> ignore)
        Assert.Contains("QueryTests.fs", ex.InnerExceptions.[0].Message)
        Assert.Contains("line: 420", ex.InnerExceptions.[0].Message)


    [<Fact>]
    let ``Templated queries``() = 

        let createProtoConnection() = 
            createConnectionMock
                [ "name", DbType.String; "statuses", DbType.String; "roles", DbType.String] 
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [ ]                            
                ]

        let qb = QueryBuilder (createConfig createProtoConnection)
               
        let query = 
            qb.TemplatedSql(
                Templating.define "select * from User u {{JOIN-CLAUSES}} {{WHERE-CLAUSE}} {{ORDER-BY-CLAUSE}}"
                    (Templating.applyWhen (fun p -> p.name.IsSome)       
                        (Templating.where "name like '%' + @name + '%'")
                     >> Templating.applyWhen (fun p -> p.statuses.IsSome)   
                        (Templating.where "status in (@statuses)")
                     >> Templating.applyWhen  (fun p -> p.roles.IsSome)
                        (Templating.join "join Role r on r.postId = u.id" >> Templating.where "r.name in (@roles)")),
                Params.Record<Criteria>(), 
                Results.Seq<User> "")               

        let connection = 
            createConnectionMock
                [ "name", DbType.String; "statuses0", DbType.String; "statuses1", DbType.String; "roles0", DbType.String; "roles1", DbType.String ] 
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]
        let command = connection.CreateCommand()
        let connector = new Connector(connection, null)        

        let criteria = 
            {
                name = None
                statuses = Some [ Status.Active; Status.New ]
                roles = Some [ "Guest"; "Tester" ]
            }

        let users = query criteria connector |> Async.RunSynchronously

        let expected = 
            [
                {
                    userId = 1
                    name = "jacentino"
                    email = "jacentino@gmail.com"
                    created = DateTime(2023, 1, 1)
                }
            ]

        Assert.Equal("select * from User u  join Role r on r.postId = u.id  where r.name in (@roles0, @roles1) and status in (@statuses0, @statuses1) ", command.CommandText)


    [<Fact>]
    let ``Custom converters - parameters``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let config = createConfig(createConnection).AddParamConverter(fun (UserId id) -> id)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql ("select * from User where userId = @Id", Params.Auto<UserId> "id", Results.Single<User> "")
                    

        let connector = new Connector(createConnection(), null)        

        let user = query (UserId 1) connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Fact>]
    let ``Custom converters - parameters - list``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [ ]                            
                ]

        let config = createConfig(createConnection).AddParamConverter(fun (UserId id) -> id)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql("select * from User where userId in (@id)", Params.Auto<UserId list> "id", Results.Single<User> "")
                

        let createConnection() = 
            createConnectionMock
                [ "id0", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let connector = new Connector(createConnection(), null)        

        let user = query [UserId 1] connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Fact>]
    let ``Custom converters - results``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let config = createConfig(createConnection).AddRowConverter(UserId)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql(
            "select * from User where userId = @Id", 
            Params.Auto<int> "id", 
            Results.Single(Rows.Tuple<UserId, string, string, DateTime>("userId", "name", "email", "created")))                

        let connector = new Connector(createConnection(), null)        

        let user = query 1 connector |> Async.RunSynchronously

        let expected = (UserId 1, "jacentino", "jacentino@gmail.com", DateTime(2023, 1, 1))

        Assert.Equal(expected, user)


    [<Fact>]
    let ``Custom converters - parameters & results``() = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let config = createConfig(createConnection)
                        .AddParamConverter(fun (UserId id) -> id)
                        .AddRowConverter(UserId)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql(
            "select * from User where userId = @Id", 
            Params.Auto<UserId> "id", 
            Results.Single(Rows.Tuple<UserId, string, string, DateTime>("userId", "name", "email", "created")))                

        let connector = new Connector(createConnection(), null)        

        let user = query (UserId 1) connector |> Async.RunSynchronously

        let expected = (UserId 1, "jacentino", "jacentino@gmail.com", DateTime(2023, 1, 1))

        Assert.Equal(expected, user)

            

    [<Fact>]
    let ``Configurators``() =
        
        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "user_userId"; col<string> "user_name"; col<string> "user_email"; col<DateTime> "user_created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                    [ col<int> "userId"; col<string> "name";  ],
                    [
                        [ 1; "Administrator" ]
                        [ 1; "Guest" ]
                    ]                            
                ]

        let config = (createConfig createConnection)
                        .AddConfigurator((fun prefix -> prefix, RecordNaming.Prefix), fun t -> t = typeof<UserWithRoles>)

        let qb = QueryBuilder (config)
        let uwr = any<UserWithRoles>

        let query = qb.Sql<int, UserWithRoles seq> (
            "select * from User where userId = @id;
             select * from Role where userId = @id",
            "id",
            Results.Seq(Rows.PKeyed<int, UserWithRoles>("user_userId", "user_")) 
            |> Results.Join uwr.roles (Results.Seq(Rows.FKeyed<int, string>("userId", "name")))
            |> Results.Map (Seq.map snd))

        let connector = new Connector(createConnection(), null)        

        let user = query 1 connector |> Async.RunSynchronously |> Seq.toList

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
                roles = [ "Administrator"; "Guest" ]
            }

        Assert.Equal<UserWithRoles seq>([expected], user)
