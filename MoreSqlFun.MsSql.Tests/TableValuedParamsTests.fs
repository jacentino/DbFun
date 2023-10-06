namespace MoreSqlFun.MsSql.Tests

open System
open System.Data
open Microsoft.Data.SqlClient.Server
open Xunit
open MoreSqlFun.MsSql.Builders
open MoreSqlFun.TestTools.Models

module TableValuedParamsTests = 

    [<Fact>]
    let ``Simple values``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = [| SqlMetaData("id", SqlDbType.Int) |]
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Simple<int>("id") (record)
        setter.SetValue(5, record)
        Assert.Equal(5, record.GetInt32(0))


    [<Fact>]
    let ``Char enums``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = [| SqlMetaData("status", SqlDbType.Char, 1) |]
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Simple<Status>("status") (record)
        setter.SetValue(Status.Blocked, record)
        Assert.Equal("B", record.GetString(0))


    [<Fact>]
    let ``Int enums``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = [| SqlMetaData("role", SqlDbType.Int) |]
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Simple<Role>("role") (record)
        setter.SetValue(Role.Regular, record)
        Assert.Equal(2, record.GetInt32(0))


    [<Fact>]
    let ``String enums``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = [| SqlMetaData("access", SqlDbType.VarChar, 2) |]
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Simple<Access>("access") (record)
        setter.SetValue(Access.ReadWrite, record)
        Assert.Equal("RW", record.GetString(0))


    [<Fact>]
    let ``Records``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = 
            [| 
                SqlMetaData("userId", SqlDbType.Int)
                SqlMetaData("name", SqlDbType.VarChar, 20)
                SqlMetaData("email", SqlDbType.VarChar, 100)
                SqlMetaData("created", SqlDbType.DateTime)
            |]
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Record<User>() (record)
        let user = 
            {
                User.userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        setter.SetValue(user, record)
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))


    [<Fact>]
    let ``Records with overrides``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = 
            [| 
                SqlMetaData("id", SqlDbType.Int)
                SqlMetaData("name", SqlDbType.VarChar, 20)
                SqlMetaData("email", SqlDbType.VarChar, 100)
                SqlMetaData("created", SqlDbType.DateTime)
            |]
        let u = Unchecked.defaultof<User>
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Record<User>(TVParamOverride(<@ u.userId @>, tvpBuilder.Simple<int>("id"))) (record)
        let user = 
            {
                User.userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        setter.SetValue(user, record)
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))


    [<Fact>]
    let ``Tuples``() =         
        let tvpBuilder = TVParamBuilder []
        let metadata = 
            [| 
                SqlMetaData("userId", SqlDbType.Int)
                SqlMetaData("name", SqlDbType.VarChar, 20)
                SqlMetaData("email", SqlDbType.VarChar, 100)
            |]
        let record = SqlDataRecord(metadata)
        let setter = tvpBuilder.Tuple<int, string, string>("userId", "name", "email") (record)
        let user = 3, "jacentino", "jacentino@gmail.com"
        setter.SetValue(user, record)
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
