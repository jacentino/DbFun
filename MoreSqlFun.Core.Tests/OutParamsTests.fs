namespace MoreSqlFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open MoreSqlFun.Core.Builders
open MoreSqlFun.Core.Tests.Models

module OutParamsTests = 

    let connection = new SqlConnection()

    [<Fact>]
    let ``Simple types``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Simple<int>("id")()

        getter.Create(command)
        command.Parameters.["id"].Value <- 5
        let value = getter.Get(command)

        Assert.Equal(5, value)
        

    [<Fact>]
    let ``Char enum types``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Simple<Status>("status")()

        getter.Create(command)
        command.Parameters.["status"].Value <- 'A'
        let value = getter.Get(command)

        Assert.Equal(Status.Active, value)
        
        
    [<Fact>]
    let ``Int enum types``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Simple<Role>("role")()

        getter.Create(command)
        command.Parameters.["role"].Value <- 1
        let value = getter.Get(command)

        Assert.Equal(Role.Guest, value)
        
        
    [<Fact>]
    let ``Attr enum types``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Simple<Access>("access")()

        getter.Create(command)
        command.Parameters.["access"].Value <- "RW"
        let value = getter.Get(command)

        Assert.Equal(Access.ReadWrite, value)
        
        
    [<Fact>]
    let ``Simple type options - Some``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Optional<int>("id")()

        getter.Create(command)
        command.Parameters.["id"].Value <- 1
        let value = getter.Get(command)

        Assert.Equal(Some 1, value)
        
        
    [<Fact>]
    let ``Simple type options - None``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Optional<int>("id")()

        getter.Create(command)
        command.Parameters.["id"].Value <- DBNull.Value
        let value = getter.Get(command)

        Assert.Equal(None, value)

        
    [<Fact>]        
    let ``Simple type tuples``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Tuple<int, string>("id", "name")()

        getter.Create(command)
        command.Parameters.["id"].Value <- 2
        command.Parameters.["name"].Value <- "jacentino"
        let value = getter.Get(command)

        Assert.Equal((2, "jacentino"), value)

        
    [<Fact>]        
    let ``Flat records``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Record<User>("")()

        getter.Create(command)
        command.Parameters.["userId"].Value <- 2
        command.Parameters.["name"].Value <- "jacentino"
        command.Parameters.["email"].Value <- "jacentino@gmail.com"
        command.Parameters.["created"].Value <- DateTime.Today
        let value = getter.Get(command)

        let expected = 
            {
                userId = 2
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime.Today
            }
        Assert.Equal(expected, value)
