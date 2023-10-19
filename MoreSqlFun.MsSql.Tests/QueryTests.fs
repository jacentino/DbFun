namespace MoreSqlFun.MsSql.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open MoreSqlFun.TestTools.Models
open MoreSqlFun.TestTools.Mocks
open MoreSqlFun.Core
open MoreSqlFun.Core.Builders
open MoreSqlFun.MsSql.Builders

module QueryTests = 

            
    [<Fact>]
    let ``Procedures``() = 

        let createConnection() = 
            setupCommandOutParams
                [   "userId", box 1
                    "name", box "jacentino"
                    "email", box "jacentino@gmail.com"
                    "created", box (DateTime(2023, 1, 1))
                    "ret_val", box 5
                ]

        let connector = new Connector(createConnection(), null)

        let qb = QueryBuilder (QueryConfig.MsSqlDefault(createConnection))
               
        let query = qb.Proc(Params.Simple<int> "id") (OutParams.ReturnAnd<User>("ret_val", "user")) Results.Unit "getUser"

        let _, (retVal, user) = query 1 connector |> Async.RunSynchronously

        let expected = 
            {
                userId = 1
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(5, retVal)
        Assert.Equal(expected, user)

    [<Fact>]
    let ``Record seq - using TVP`` () =
    
        let createConnection() = createConnectionMock [] //[]

        let createConnection () = 
            createConnectionMock              
                []
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let connector = new Connector(createConnection(), null)
        let qb = QueryBuilder({ QueryConfig.MsSqlDefault(createConnection) with ParamBuilders = ParamsImpl.getDefaultBuilders(createConnection) })
        let query = qb.Timeout(30).Sql(Params.TableValuedSeq<User>("users")) Results.Unit 
                        "insert into User (userId, name, email, created) 
                         select userId, name, email, created from @users"

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }

        query [user] connector |> Async.RunSynchronously
