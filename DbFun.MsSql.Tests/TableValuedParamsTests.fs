namespace DbFun.MsSql.Tests

open System
open System.Data
open Microsoft.Data.SqlClient.Server
open Xunit
open DbFun.MsSql.Builders
open DbFun.TestTools.Models
open DbFun.Core.Builders.GenericSetters
open DbFun.Core.Builders
open DbFun.Core.Builders.Compilers

module TableValuedParamsTests = 

    [<Fact>]
    let ``Simple values``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = [| SqlMetaData("id", SqlDbType.Int) |]
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Auto<int>("id") (tvpProvider, record)
        setter.SetValue(5, None, record)
        Assert.Equal(5, record.GetInt32(0))


    [<Fact>]
    let ``Char enums``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = [| SqlMetaData("status", SqlDbType.Char, 1) |]
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Auto<Status>("status") (tvpProvider, record)
        setter.SetValue(Status.Blocked, None, record)
        Assert.Equal("B", record.GetString(0))


    [<Fact>]
    let ``Int enums``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = [| SqlMetaData("role", SqlDbType.Int) |]
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Auto<Role>("role") (tvpProvider, record)
        setter.SetValue(Role.Regular, None, record)
        Assert.Equal(2, record.GetInt32(0))


    [<Fact>]
    let ``String enums``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = [| SqlMetaData("access", SqlDbType.VarChar, 2) |]
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Auto<Access>("access") (tvpProvider, record)
        setter.SetValue(Access.ReadWrite, None, record)
        Assert.Equal("RW", record.GetString(0))


    [<Fact>]
    let ``Records``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = 
            [| 
                SqlMetaData("userId", SqlDbType.Int)
                SqlMetaData("name", SqlDbType.VarChar, 20)
                SqlMetaData("email", SqlDbType.VarChar, 100)
                SqlMetaData("created", SqlDbType.DateTime)
            |]
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Record<User>() (tvpProvider, record)
        let user = 
            {
                User.userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        setter.SetValue(user, None, record)
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))


    [<Fact>]
    let ``Records with overrides``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = 
            [| 
                SqlMetaData("id", SqlDbType.Int)
                SqlMetaData("name", SqlDbType.VarChar, 20)
                SqlMetaData("email", SqlDbType.VarChar, 100)
                SqlMetaData("created", SqlDbType.DateTime)
            |]
        let u = any<User>
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Record<User>(overrides = [ TVParamOverride(u.userId, TVParams.Auto<int>("id")) ]) (tvpProvider, record)
        let user = 
            {
                User.userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        setter.SetValue(user, None, record)
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))


    [<Fact>]
    let ``Tuples``() =         
        let tvpProvider = BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
        let metadata = 
            [| 
                SqlMetaData("userId", SqlDbType.Int)
                SqlMetaData("name", SqlDbType.VarChar, 20)
                SqlMetaData("email", SqlDbType.VarChar, 100)
            |]
        let record = SqlDataRecord(metadata)
        let setter = TVParams.Tuple<int, string, string>("userId", "name", "email") (tvpProvider, record)
        let user = 3, "jacentino", "jacentino@gmail.com"
        setter.SetValue(user, None, record)
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
