namespace Sql2Fun.Core.Tests

open System
open Xunit
open Sql2Fun.Core
open Sql2Fun.Core.Builders
open Sql2Fun.Core.Builders.MultipleResults
open Sql2Fun.TestTools
open Sql2Fun.TestTools.Models
open Sql2Fun.TestTools.Mocks
open Sql2Fun.Core.Diagnostics
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
               
        let query = 
            qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
            <| "select * from User where userId = @Id"

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

        let query = 
            qb.Sql <| Params.Simple<int>("id") <| Results.Multiple(Results.One<User>(""), Results.Many<string>("name"))
            <| "select * from User where userId = @id;
                select * from Role where userId = @id"

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

        let query = 
            qb.Sql(Params.Simple<int>("id"))
                  (Results.Combine(fun user roles -> user, roles)
                    <*> Results.One<User>("") 
                    <*> Results.Many<string>("name"))
                "select * from User where userId = @id;
                 select * from Role where userId = @id"

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

        let query = 
            qb.Sql (Params.Simple<int>("id"))
                   (Results.Many(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
                    |> Results.Join (fun (u, rs) -> { u with UserWithRoles.roles = rs }) (Results.Many(Rows.FKeyed<int, string>("userId", "name")))
                    |> Results.Map (Seq.map snd))
                "select * from User where userId = @id;
                 select * from Role where userId = @id"

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
            qb.Sql (Params.Simple<int>("id"))
                   (Results.Many(Rows.PKeyed<int, UserWithRoles>("userId", "user")) 
                    |> Results.Join uwr.roles (Results.Many(Rows.FKeyed<int, string>("userId", "name")))
                    |> Results.Map (Seq.map snd))
                "select * from User where userId = @id;
                 select * from Role where userId = @id"

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
    let ``Procedures``() = 

        let createConnection() = 
            setupCommandOutParams
                [   "userId", box 1
                    "name", box "jacentino"
                    "email", box "jacentino@gmail.com"
                    "created", box (DateTime(2023, 1, 1))
                ]

        let qb = QueryBuilder (createConfig createConnection)
               
        let query = qb.Proc(Params.Simple<int> "id") (OutParams.Record<User>()) Results.Unit "getUser"

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
            Assert.Throws<CompileTimeException>(fun () -> 
                qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
                       <| "select * from User where userId = @Id"
                |> ignore)
        Assert.Contains("QueryTests.fs", ex.Message)
        Assert.Contains("line: 279", ex.Message)


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
               
        qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
               <| "select * from User where userId = @Id"
        |> ignore

        let line, file, _ = qb.CompileTimeErrorLog |> List.head

        Assert.Equal(302, line)
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
               
        let query = 
            qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
                   <| "select * from User where userId = @Id"

        let connector = new Connector(createConnection(), null)        

        let ex = Assert.Throws<AggregateException>(fun () -> query  1 connector |> Async.RunSynchronously |> ignore)
        Assert.Contains("QueryTests.fs", ex.InnerExceptions.[0].Message)
        Assert.Contains("line: 329", ex.InnerExceptions.[0].Message)

    let shouldExpand f v =
        v |> Option.map (f >> Option.isSome) |> Option.defaultValue true

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
            qb.TemplatedSql <| Params.Record<Criteria>() <| Results.Many<User> ""
            <| Templating.define "select * from User u {{JOIN-CLAUSES}} {{WHERE-CLAUSE}} {{ORDER-BY-CLAUSE}}"
                (Templating.applyWhen (fun p -> p.name.IsSome)       
                    (Templating.where "name like '%' + @name + '%'")
                 >> Templating.applyWhen (fun p -> p.statuses.IsSome)   
                    (Templating.where "status in (@statuses)")
                 >> Templating.applyWhen  (fun p -> p.roles.IsSome)      
                    (Templating.join "join Role r on r.postId = u.id" >> Templating.where "r.name in (@roles)"))

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
