namespace Sql2Fun.MsSql.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open Sql2Fun.Core.Builders
open Sql2Fun.MsSql.Builders
open System.Data
open Sql2Fun.TestTools.Models

module OutParamsTests = 

    let connection = new SqlConnection()
    let provider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(OutParamsImpl.getDefaultBuilders())
    let builderParams = provider :> IOutParamGetterProvider, ()

    [<Fact>]
    let ``Return alone``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.Return("ret_value") builderParams

        getter.Create(command)
        command.Parameters.["ret_value"].Value <- 5
        let value = getter.Get(command)

        Assert.Equal(ParameterDirection.ReturnValue, command.Parameters.["ret_value"].Direction)
        Assert.Equal(5, value)


    [<Fact>]
    let ``Return and simple output``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.ReturnAnd<string>("ret_value", "name") builderParams

        getter.Create(command)
        command.Parameters.["ret_value"].Value <- 5
        command.Parameters.["name"].Value <- "jacentino"
        let retVal, name = getter.Get(command)

        Assert.Equal(ParameterDirection.ReturnValue, command.Parameters.["ret_value"].Direction)
        Assert.Equal(5, retVal)
        Assert.Equal("jacentino", name)


    [<Fact>]
    let ``Return and record output``() = 

        let command = connection.CreateCommand()
        let getter = OutParams.ReturnAnd<User>("ret_value", "user") builderParams

        getter.Create(command)
        command.Parameters.["ret_value"].Value <- 5
        command.Parameters.["userId"].Value <- 12
        command.Parameters.["name"].Value <- "jacentino"
        command.Parameters.["email"].Value <- "jacentino@gmail.com"
        command.Parameters.["created"].Value <- DateTime(2023, 1, 1)
        let retVal, user = getter.Get(command)

        let expected = 
            {
                userId = 12
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }

        Assert.Equal(ParameterDirection.ReturnValue, command.Parameters.["ret_value"].Direction)
        Assert.Equal(5, retVal)
        Assert.Equal(expected, user)


