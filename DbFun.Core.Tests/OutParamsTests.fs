namespace DbFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open DbFun.Core.Builders
open DbFun.TestTools.Models
open System.Data

module OutParamsTests = 

    let connection = new SqlConnection()
    let provider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(OutParamsImpl.getDefaultBuilders())
    let builderParams = provider :> IOutParamGetterProvider, ()

    [<Fact>]
    let ``Simple types``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Simple<int>("id") builderParams

        getter.Create(command)
        command.Parameters.["id"].Value <- 5
        let value = getter.Get(command)

        Assert.Equal(5, value)
        

    [<Fact>]
    let ``Char enum types``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Simple<Status>("status") builderParams

        getter.Create(command)
        command.Parameters.["status"].Value <- 'A'
        let value = getter.Get(command)

        Assert.Equal(Status.Active, value)
        
        
    [<Fact>]
    let ``Int enum types``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Simple<Role>("role") builderParams

        getter.Create(command)
        command.Parameters.["role"].Value <- 1
        let value = getter.Get(command)

        Assert.Equal(Role.Guest, value)
        
        
    [<Fact>]
    let ``Discriminated union types - simple``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Union<Access>("access") builderParams

        getter.Create(command)
        command.Parameters.["access"].Value <- "RW"
        let value = getter.Get(command)

        Assert.Equal(Access.ReadWrite, value)
        
        
    [<Fact>]
    let ``Simple type options - Some``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Optional<int>("id") builderParams

        getter.Create(command)
        command.Parameters.["id"].Value <- 1
        let value = getter.Get(command)

        Assert.Equal(Some 1, value)
        
        
    [<Fact>]
    let ``Simple type options - None``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Optional<int>("id") builderParams

        getter.Create(command)
        command.Parameters.["id"].Value <- DBNull.Value
        let value = getter.Get(command)

        Assert.Equal(None, value)

        
    [<Fact>]        
    let ``Simple type tuples``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Tuple<int, string>("id", "name") builderParams

        getter.Create(command)
        command.Parameters.["id"].Value <- 2
        command.Parameters.["name"].Value <- "jacentino"
        let value = getter.Get(command)

        Assert.Equal((2, "jacentino"), value)

        
    [<Fact>]        
    let ``Flat records``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Record<User>() builderParams

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


    [<Fact>]        
    let ``Flat records - prefixed names``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Record<User>("user_") builderParams

        getter.Create(command)
        command.Parameters.["user_userId"].Value <- 2
        command.Parameters.["user_name"].Value <- "jacentino"
        command.Parameters.["user_email"].Value <- "jacentino@gmail.com"
        command.Parameters.["user_created"].Value <- DateTime.Today
        let value = getter.Get(command)

        let expected = 
            {
                userId = 2
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime.Today
            }
        Assert.Equal(expected, value)


    [<Fact>]        
    let ``Flat records - overrides``() = 

        let command = connection.CreateCommand()
        let u = any<User>
        let getter = OutParams.Record<User>(OutParamOverride<int>(u.userId, OutParams.Simple<int>("id"))) builderParams

        getter.Create(command)
        command.Parameters.["id"].Value <- 2
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