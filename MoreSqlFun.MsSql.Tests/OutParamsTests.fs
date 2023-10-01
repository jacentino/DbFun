namespace MoreSqlFun.MsSql.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open MoreSqlFun.MsSql.Builders
open System.Data
open MoreSqlFun.TestTools.Models

module OutParamsTests = 

    let connection = new SqlConnection()


    [<Fact>]
    let ``Return alone``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.Return("ret_value")()

        getter.Create(command)
        command.Parameters.["ret_value"].Value <- 5
        let value = getter.Get(command)

        Assert.Equal(ParameterDirection.ReturnValue, command.Parameters.["ret_value"].Direction)
        Assert.Equal(5, value)


    [<Fact>]
    let ``Return and simple output``() = 

        let command = connection.CreateCommand()
        let b = OutParamBuilder []
        let getter = b.ReturnAnd<string>("ret_value", "name")()

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
        let b = OutParamBuilder []
        let getter = b.ReturnAnd<User>("ret_value", "user")()

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


