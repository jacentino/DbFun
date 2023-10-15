namespace MoreSqlFun.Core.Tests

open System
open Xunit
open MoreSqlFun.Core
open MoreSqlFun.Core.Builders
open MoreSqlFun.TestTools.Models
open MoreSqlFun.TestTools.Mocks
open Microsoft.Data.SqlClient
open System.Data
open MoreSqlFun.Core.Diagnostics

module QueryTests = 

    let connector = new Connector(new SqlConnection(), null)

    let createConfig executor = { QueryConfig.Default(fun () -> new SqlConnection()) with Executor = executor }

    [<Fact>]
    let ``Simple queries``() = 

        let executor = createCommandExecutorMock
                        [ "id", DbType.Int32 ]
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let qb = QueryBuilder (createConfig executor)
               
        let query = 
            qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
            <| "select * from User where userId = @Id"

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
        
        let executor = createCommandExecutorMock
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

        let qb = QueryBuilder (createConfig executor)

        let x = Results.Multiple(Results.One<User>(""), Results.Many<string>("name"))

        let query = 
            qb.Sql <| Params.Simple<int>("id") <| Results.Multiple(Results.One<User>(""), Results.Many<string>("name"))
            <| "select * from User where userId = @id;
                select * from Role where userId = @id"

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
    let ``Joins with lambda merge``() =
        
        let executor = createCommandExecutorMock
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

        let qb = QueryBuilder (createConfig executor)

        let query = 
            qb.Sql (Params.Simple<int>("id"))
                   (Results.Many(Rows.PK<int, UserWithRoles>("userId", "user")) 
                    |> Results.Join (fun (u, rs) -> { u with roles = rs }) (Results.Many(Rows.FK<int, string>("userId", "name")))
                    |> Results.Map (Seq.map snd))
                "select * from User where userId = @id;
                 select * from Role where userId = @id"

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
        
        let executor = createCommandExecutorMock
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

        let qb = QueryBuilder (createConfig executor)
        let uwr = any<UserWithRoles>

        let query = 
            qb.Sql (Params.Simple<int>("id"))
                   (Results.Many(Rows.PK<int, UserWithRoles>("userId", "user")) 
                    |> Results.Join uwr.roles (Results.Many(Rows.FK<int, string>("userId", "name")))
                    |> Results.Map (Seq.map snd))
                "select * from User where userId = @id;
                 select * from Role where userId = @id"

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

        let executor = setupCommandOutParams
                        [   "userId", box 1
                            "name", box "jacentino"
                            "email", box "jacentino@gmail.com"
                            "created", box (DateTime(2023, 1, 1))
                        ]

        let qb = QueryBuilder (createConfig executor)
               
        let query = qb.Proc(Params.Simple<int> "id") (OutParams.Record<User>()) Results.Unit "getUser"

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

        let executor = createCommandExecutorMock
                        [ "wrongName", DbType.Int32 ]
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let qb = QueryBuilder (createConfig executor)
               
        let ex = 
            Assert.Throws<CompileTimeException>(fun () -> 
                qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
                       <| "select * from User where userId = @Id"
                |> ignore)
        Assert.Contains("QueryTests.fs", ex.Message)
        Assert.Contains("line: 221", ex.Message)


    [<Fact>]
    let ``Compile-time errors - logging``() = 

        let executor = createCommandExecutorMock
                        [ "wrongName", DbType.Int32 ]
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let qb = QueryBuilder(createConfig executor).LogCompileTimeErrors()
               
        qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
                <| "select * from User where userId = @Id"
        |> ignore

        let line, file, _ = qb.CompileTimeErrorLog |> List.head

        Assert.Equal(243, line)
        Assert.Contains("QueryTests.fs", file)


    [<Fact>]
    let ``Compile-time errors - deferred throw``() = 

        let executor = createCommandExecutorMock
                        [ "wrongName", DbType.Int32 ]
                        [
                            [ col<int> "userId"; col<string> "name"; col<string> "email"; col<DateTime> "created" ],
                            [
                                [ 1; "jacentino"; "jacentino@gmail.com"; DateTime(2023, 1, 1) ]
                            ]
                            
                        ]

        let qb = QueryBuilder(createConfig executor).LogCompileTimeErrors()
               
        let query = 
            qb.Sql <| Params.Simple<int> "id" <| Results.One<User> ""
                   <| "select * from User where userId = @Id"

        let ex = Assert.Throws<AggregateException>(fun () -> query  1 connector |> Async.RunSynchronously |> ignore)
        Assert.Contains("QueryTests.fs", ex.InnerExceptions.[0].Message)
        Assert.Contains("line: 269", ex.InnerExceptions.[0].Message)