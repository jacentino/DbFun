namespace MoreSqlFun.Core.Tests

open System
open Xunit
open MoreSqlFun.Core
open MoreSqlFun.Core.Builders
open MoreSqlFun.TestTools.Models
open MoreSqlFun.TestTools.Mocks
open Microsoft.Data.SqlClient
open System.Data

module QueryTests = 

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

        let connector = new Connector(new SqlConnection(), null)

        let pb = ParamBuilder []
        let rb = RowBuilder []
        let rs = ResultBuilder rb
        let qb = QueryBuilder ((fun () -> new SqlConnection()), executor)
               
        let query = 
            qb.Sql <| pb.Simple<int> "id" <| rs.One<User> ""
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

        let connector = new Connector(new SqlConnection(), null)

        let pb = ParamBuilder []
        let rb = RowBuilder []
        let rs = ResultBuilder rb
        let qb = QueryBuilder ((fun () -> new SqlConnection()), executor)

        let query = 
            qb.Sql <| pb.Simple<int>("id") <| rs.Multiple(rs.One<User>(""), rs.Many<string>("name"))
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


    let any<'R> : 'R = Unchecked.defaultof<'R>

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

        let connector = new Connector(new SqlConnection(), null)

        let pb = ParamBuilder []
        let rb = RowBuilder []
        let rs = ResultBuilder rb
        let qb = QueryBuilder ((fun () -> new SqlConnection()), executor)

        let query = 
            qb.Sql (pb.Simple<int>("id"))
                   (rs.Many(rb.PK<int, UserWithRoles>("userId", "user")) 
                    |> rs.Join (fun (u, rs) -> { u with roles = rs }) (rs.Many(rb.FK<int, string>("userId", "name")))
                    |> rs.Map (Seq.map snd))
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

        let connector = new Connector(new SqlConnection(), null)

        let pb = ParamBuilder []
        let rb = RowBuilder []
        let rs = ResultBuilder rb
        let qb = QueryBuilder ((fun () -> new SqlConnection()), executor)
        let uwr = any<UserWithRoles>

        let query = 
            qb.Sql (pb.Simple<int>("id"))
                   (rs.Many(rb.PK<int, UserWithRoles>("userId", "user")) 
                    |> rs.Join <@ uwr.roles @> (rs.Many(rb.FK<int, string>("userId", "name")))
                    |> rs.Map (Seq.map snd))
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

        let connector = new Connector(new SqlConnection(), null)

        let pb = ParamBuilder []
        let opb = OutParamBuilder []
        let rb = RowBuilder []
        let rs = ResultBuilder rb
        let qb = QueryBuilder ((fun () -> new SqlConnection()), executor)
               
        let query = qb.Proc(pb.Simple<int> "id") (opb.Record<User>("user")) rs.Unit "getUser"

        let _, user = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(expected, user)
