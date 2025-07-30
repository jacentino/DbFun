namespace DbFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open DbFun.Core.Builders
open DbFun.FastExpressionCompiler.Compilers
open DbFun.TestTools.Models
open DbFun.Core.Builders.Compilers
open DbFun.Core.Builders.GenericSetters
open System.Data

module OutParamsTests = 

    let connection = new SqlConnection()

    let compilers: ICompiler list = [ LinqExpressionCompiler(); Compiler()]

    let providers = 
        compilers 
        |> List.map (fun compiler -> [|
            GenericGetters.BaseGetterProvider<unit, IDbCommand>(OutParamsImpl.getDefaultBuilders(), compiler)
        |])

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Simple types``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Auto("id") (provider, ())

        getter.Create(command)
        command.Parameters.["id"].Value <- 5
        let value = getter.Get(command)

        Assert.Equal(5, value)
        

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Char enum types``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Auto("status") (provider, ())

        getter.Create(command)
        command.Parameters.["status"].Value <- 'A'
        let value = getter.Get(command)

        Assert.Equal(Status.Active, value)
        
        
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Int enum types``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Auto("role") (provider, ())

        getter.Create(command)
        command.Parameters.["role"].Value <- 1
        let value = getter.Get(command)

        Assert.Equal(Role.Guest, value)
        
        
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated union types - simple``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Union<Access>("access") (provider, ())

        getter.Create(command)
        command.Parameters.["access"].Value <- "RW"
        let value = getter.Get(command)

        Assert.Equal(Access.ReadWrite, value)
        
        
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Simple type options - Some``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Optional<int>("id") (provider, ())

        getter.Create(command)
        command.Parameters.["id"].Value <- 1
        let value = getter.Get(command)

        Assert.Equal(Some 1, value)
        
        
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Simple type options - None``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Optional<int>("id") (provider, ())

        getter.Create(command)
        command.Parameters.["id"].Value <- DBNull.Value
        let value = getter.Get(command)

        Assert.Equal(None, value)

        
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Simple type tuples``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Tuple<int, string>("id", "name") (provider, ())

        getter.Create(command)
        command.Parameters.["id"].Value <- 2
        command.Parameters.["name"].Value <- "jacentino"
        let value = getter.Get(command)

        Assert.Equal((2, "jacentino"), value)

        
    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Record<User>() (provider, ())

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


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records - prefixed names``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let getter = OutParams.Record<User>("user_", RecordNaming.Prefix) (provider, ())

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


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records - overrides``(provider: GenericGetters.BaseGetterProvider<unit, IDbCommand>) = 

        let command = connection.CreateCommand()
        let u = any<User>
        let getter = OutParams.Record<User>(overrides = [OutParamOverride<int>(u.userId, OutParams.Auto("id"))]) (provider, ())

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