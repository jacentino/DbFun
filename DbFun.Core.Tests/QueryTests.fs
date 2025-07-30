namespace DbFun.Core.Tests

open System
open Xunit
open DbFun.Core
open DbFun.Core.Builders
open DbFun.FastExpressionCompiler
open DbFun.Core.Builders.MultipleResults
open DbFun.TestTools
open DbFun.TestTools.Models
open DbFun.TestTools.Mocks
open DbFun.Core.Diagnostics
open System.Data
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open DbFun.Core.Builders.Compilers
open DbFun.FastExpressionCompiler.Compilers

type Diag() = 
    static member GetLine([<CallerLineNumber; Optional; DefaultParameterValue(0)>] line: int) = line

module QueryTests = 

    let compilers: ICompiler array list = [ [| LinqExpressionCompiler() |]; [| Compiler() |] ]

    let createConfig createConnection compiler = { QueryConfig.Default(createConnection) with Compiler = compiler }

    [<Theory>][<MemberData(nameof compilers)>]
    let ``Simple queries``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)
               
        let query = qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")

        let connector = new Connector(createConnection)        

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Simple queries - param by name``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql<int, User>("select * from User where userId = @Id", "id", Results.Auto()) 

        let connector = new Connector(createConnection)        

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Simple queries - simplest form``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql<int, User>("select * from User where userId = @Id", "id") 

        let connector = new Connector(createConnection)

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Multiple results``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql(
            "select * from User where userId = @id;
             select * from Role where userId = @id",
            Params.Auto<int>("id"), 
            Results.Multiple<User, string seq>("", "name"))

        let connector = new Connector(createConnection)

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


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Multiple results applicative``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql(
                "select * from User where userId = @id;
                 select * from Role where userId = @id",
                Params.Auto<int>("id"),
                Results.Combine(fun user roles -> user, roles)
                <*> Results.Single<User>("") 
                <*> Results.Seq<string>("name"))                

        let connector = new Connector(createConnection)

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


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Joins with lambda merge``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql (
            "select * from User where userId = @id;
             select * from Role where userId = @id",
            Params.Auto<int>("id"),
            Results.Seq(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
            |> Results.Join (fun (u, rs) -> { u with UserWithRoles.roles = rs }) (Results.Seq(Rows.FKeyed<int, string>("userId", "name")))
            |> Results.Map (Seq.map snd))
                

        let connector = new Connector(createConnection)

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
            

    [<Theory>][<MemberData(nameof compilers)>]
    let ``Joins with expr merge``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)
        let uwr = any<UserWithRoles>

        let query = 
             qb.Sql (
                "select * from User where userId = @id;
                 select * from Role where userId = @id",             
                Params.Auto<int>("id"),
                Results.Seq(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
                |> Results.Join uwr.roles (Results.Seq(Rows.FKeyed<int, string>("userId", "name")))
                |> Results.Unkeyed)
                

        let connector = new Connector(createConnection)

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
            

    [<Theory>][<MemberData(nameof compilers)>]
    let ``Joins with lambda expr merge``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = 
             qb.Sql (
                "select * from User where userId = @id;
                 select * from Role where userId = @id",             
                Params.Auto<int>("id"),
                Results.Seq(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
                |> Results.Join (fun u -> u.roles) (Results.Seq(Rows.FKeyed<int, string>("userId", "name")))
                |> Results.Unkeyed)
                

        let connector = new Connector(createConnection)

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


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Grouping with lambda merge``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql (
            "select u.*, r.name as roleName from User u left join Role r on u.userId = r.userId
             where u.userId = @id",
            Params.Auto<int>("id"),                        
            Results.List(Rows.Tuple<UserWithRoles, string option>("user", "roleName"))
            |> Results.Group (fun (u: UserWithRoles) rs -> { u with roles = rs }))

        let connector = new Connector(createConnection)

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


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Grouping with expr merge``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql (
            "select u.*, r.name as roleName from User u left join Role r on u.userId = r.userId
             where u.userId = @id",
            Params.Auto<int>("id"),                        
            Results.List(Rows.Tuple<UserWithRoles, string option>("user", "roleName"))
            |> Results.Group any<UserWithRoles>.roles)

        let connector = new Connector(createConnection)

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


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Grouping with lambda expr merge``(compiler) =
        
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

        let qb = QueryBuilder (createConfig createConnection compiler)

        let query = qb.Sql (
            "select u.*, r.name as roleName from User u left join Role r on u.userId = r.userId
             where u.userId = @id",
            Params.Auto<int>("id"),                        
            Results.List(Rows.Tuple<UserWithRoles, string option>("user", "roleName"))
            |> Results.Group (fun u -> u.roles))

        let connector = new Connector(createConnection)

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

            
    [<Theory>][<MemberData(nameof compilers)>]
    let ``Procedures``(compiler) = 

        let createConnection() = 
            setupCommandOutParams
                [   "userId", box 1
                    "name", box "jacentino"
                    "email", box "jacentino@gmail.com"
                    "created", box (DateTime(2023, 1, 1))
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)
               
        let query = qb.Proc("getUser", Params.Auto<int> "id", OutParams.Record<User>(), Results.Unit) 

        let connector = new Connector(createConnection)

        let _, user = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)

            
    [<Theory>][<MemberData(nameof compilers)>]
    let ``Procedures - simplest form``(compiler) = 

        let createConnection() = 
            setupCommandOutParams
                [   "userId", box 1
                    "name", box "jacentino"
                    "email", box "jacentino@gmail.com"
                    "created", box (DateTime(2023, 1, 1))
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)
               
        let query = qb.Proc<int, User, unit>("getUser", "id") 

        let connector = new Connector(createConnection)

        let _, user = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)

    [<Theory>][<MemberData(nameof compilers)>]
    let ``Compile-time errors - immediately``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                            
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)
               
        let line = Diag.GetLine()
        let ex = Assert.Throws<CompileTimeException>(fun () -> qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "") |> ignore)
        Assert.Contains("QueryTests.fs", ex.Message)
        Assert.Contains(sprintf "line %d" (line + 1), ex.Message)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Compile-time errors - logging``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]
                            
                ]

        let qb = QueryBuilder(createConfig createConnection compiler).LogCompileTimeErrors()
               
        let line = Diag.GetLine()
        qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")               
        |> ignore

        let lineNo, fileName, _ = qb.CompileTimeErrors |> List.head

        Assert.Equal(line + 1, lineNo)
        Assert.Contains("QueryTests.fs", fileName)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Compile-time errors - deferred throw``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder(createConfig createConnection compiler).LogCompileTimeErrors()
               
        let line = Diag.GetLine()
        let query = qb.Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")                   

        let connector = new Connector(createConnection)

        let ex = Assert.Throws<AggregateException>(fun () -> query  1 connector |> Async.RunSynchronously |> ignore)
        Assert.Contains("QueryTests.fs", ex.InnerExceptions.[0].Message)
        Assert.Contains(sprintf "line %d" (line + 1), ex.InnerExceptions.[0].Message)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Compile-time errors - logging & derived QueryBuilder``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "wrongName", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder(createConfig createConnection compiler).LogCompileTimeErrors()
               
        let line = Diag.GetLine()
        qb.Timeout(30).Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")               
        |> ignore

        let lineNo, fileName, _ = qb.CompileTimeErrors |> List.head

        Assert.Equal(line + 1, lineNo)
        Assert.Contains("QueryTests.fs", fileName)


    type Criteria = 
        {
            name    : string option
            roles   : string list option
            statuses: Status list option
        }


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Templated queries``(compiler) = 

        let createProtoConnection() = 
            createConnectionMock
                [ "name", DbType.String; "statuses", DbType.String; "roles", DbType.String] 
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [ ]                            
                ]

        let qb = QueryBuilder((createConfig createProtoConnection compiler).HandleCollectionParams())
               
        let query = 
            qb.Sql(
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
        let connector = new Connector((fun () -> failwith "Cloning is not supported"), ref[ (), connection ], [])

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
        Assert.Equal<User>(expected, users)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Templated batches``(compiler) = 

        let createProtoConnection() = 
            createConnectionMock
                [ "userId", DbType.Int32; "name", DbType.String; "email", DbType.String; "created", DbType.DateTime] 
                [
                    [ ],
                    [ ]                            
                ]

        let qb = QueryBuilder ((createConfig createProtoConnection compiler).HandleCollectionParams())

        let query = qb.Sql<User list, unit>(
            Templating.define "insert into User (userId, name, email, created) values {{VALUES}}"
                (Templating.applyWith List.length
                    (Templating.enumerate "VALUES" "(@userId{{IDX}}, @name{{IDX}}, @email{{IDX}}, @created{{IDX}})" ", "))
            )

        let connection = 
            createConnectionMock
                [   "userId0", DbType.Int32; "name0", DbType.String; "email0", DbType.String; "created0", DbType.DateTime
                    "userId1", DbType.Int32; "name1", DbType.String; "email1", DbType.String; "created1", DbType.DateTime
                ] 
                [
                    [ ],
                    [ ]                            
                ]
    
        let command = connection.CreateCommand()
        let connector = new Connector(fun () -> connection)

        let users = [
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime(2024, 1, 1) }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime(2024, 2, 1) }
        ]

        query users connector |> Async.RunSynchronously

        Assert.Equal(
            "insert into User (userId, name, email, created) values (@userId0, @name0, @email0, @created0), (@userId1, @name1, @email1, @created1)", 
            command.CommandText)
        Assert.Equal(box 1, (command.Parameters["userId0"] :?> IDataParameter).Value)
        Assert.Equal(box 2, (command.Parameters["userId1"] :?> IDataParameter).Value)
        Assert.Equal(box "jacenty", (command.Parameters["name0"] :?> IDataParameter).Value)
        Assert.Equal(box "placenty", (command.Parameters["name1"] :?> IDataParameter).Value)
        Assert.Equal(box 2, (command.Parameters["userId1"] :?> IDataParameter).Value)
        Assert.Equal(box "jacenty@gmail.com", (command.Parameters["email0"] :?> IDataParameter).Value)
        Assert.Equal(box "placenty@gmail.com", (command.Parameters["email1"] :?> IDataParameter).Value)
        Assert.Equal(box (DateTime(2024, 1, 1)), (command.Parameters["created0"] :?> IDataParameter).Value)
        Assert.Equal(box (DateTime(2024, 2, 1)), (command.Parameters["created1"] :?> IDataParameter).Value)
    

    [<Theory>][<MemberData(nameof compilers)>]
    let ``Custom converters - parameters``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let config = (createConfig createConnection compiler).AddParamConverter(fun (UserId id) -> id)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql ("select * from User where userId = @Id", Params.Auto<UserId> "id", Results.Single<User> "")
                    

        let connector = new Connector(createConnection)

        let user = query (UserId 1) connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Custom converters - parameters - list``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [ ]                            
                ]

        let config = (createConfig createConnection compiler)
                        .AddParamConverter(fun (UserId id) -> id)
                        .HandleCollectionParams()

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

        let connector = new Connector(createConnection)

        let user = query [UserId 1] connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Custom converters - results``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let config = (createConfig createConnection compiler).AddRowConverter(UserId)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql(
            "select * from User where userId = @Id", 
            Params.Auto<int> "id", 
            Results.Single(Rows.Tuple<UserId, string, string, DateTime>("userId", "name", "email", "created")))                

        let connector = new Connector(createConnection)

        let user = query 1 connector |> Async.RunSynchronously

        let expected = (UserId 1, "jacentino", "jacentino@gmail.com", DateTime(2023, 1, 1))

        Assert.Equal(expected, user)


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Custom converters - parameters & results``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let config = (createConfig createConnection compiler)
                        .AddParamConverter(fun (UserId id) -> id)
                        .AddRowConverter(UserId)

        let qb = QueryBuilder(config)
               
        let query = qb.Sql(
            "select * from User where userId = @Id", 
            Params.Auto<UserId> "id", 
            Results.Single(Rows.Tuple<UserId, string, string, DateTime>("userId", "name", "email", "created")))                

        let connector = new Connector(createConnection)

        let user = query (UserId 1) connector |> Async.RunSynchronously

        let expected = (UserId 1, "jacentino", "jacentino@gmail.com", DateTime(2023, 1, 1))

        Assert.Equal(expected, user)

            

    [<Theory>][<MemberData(nameof compilers)>]
    let ``Configurators``(compiler) =
        
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

        let config = (createConfig createConnection compiler)
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

        let connector = new Connector(createConnection)

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


    [<Theory>][<MemberData(nameof compilers)>]
    let ``Simple queries - no prototype calls``(compiler) = 

        let createConnection() = 
            createConnectionMock
                [ "id", DbType.Int32 ]
                [
                    [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                    [
                        [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                    ]                            
                ]

        let qb = QueryBuilder (createConfig createConnection compiler)
               
        let query = qb.DisablePrototypeCalls().Sql("select * from User where userId = @Id", Params.Auto<int> "id", Results.Single<User> "")

        let connector = new Connector(createConnection)        

        let user = query  1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)
