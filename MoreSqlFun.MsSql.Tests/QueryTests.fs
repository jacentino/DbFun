namespace MoreSqlFun.MsSql.Tests

open System
open Xunit
open System.Data
open Microsoft.Data.SqlClient
open MoreSqlFun.TestTools.Models
open MoreSqlFun.TestTools.Mocks
open MoreSqlFun.Core
open MoreSqlFun.Core.Builders
open MoreSqlFun.MsSql.Builders

module QueryTests = 

            
    [<Fact>]
    let ``Procedures``() = 

        let executor = setupCommandOutParams
                        [   "userId", box 1
                            "name", box "jacentino"
                            "email", box "jacentino@gmail.com"
                            "created", box (DateTime(2023, 1, 1))
                            "ret_val", box 5
                        ]

        let connector = new Connector(new SqlConnection(), null)

        let tvpBuilder = TVParamBuilder []
        let pb = ParamBuilder([], (fun () -> new SqlConnection()), tvpBuilder)
        let opb = OutParamBuilder []
        let rb = RowBuilder []
        let rs = ResultBuilder rb
        let qb = QueryBuilder ((fun () -> new SqlConnection()), executor)
               
        let query = qb.Proc(pb.Simple<int> "id") (opb.ReturnAnd<User>("ret_val", "user")) rs.Unit "getUser"

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


