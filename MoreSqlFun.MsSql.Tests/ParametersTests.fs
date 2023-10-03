namespace MoreSqlFun.MsSql.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open MoreSqlFun.MsSql.Builders
open MoreSqlFun.TestTools.Models
open MoreSqlFun.TestTools.Mocks
open Microsoft.Data.SqlClient.Server

module ParametersTests = 

    let connection = new SqlConnection()

    [<Fact>]
    let ``Record seq - explicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.TableValuedSeq<User>("users")()).SetValue([user], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record seq - implicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.Simple<User seq>("users")()).SetValue([user], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record list - explicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.TableValuedList<User>("users")()).SetValue([user], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record list - implicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.Simple<User list>("users")()).SetValue([user], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record array - explicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.TableValuedArray<User>("users")()).SetValue([| user |], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record array - implicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.Simple<User array>("users")()).SetValue([| user |], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record seq - super explicit`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        let tvp = tvpBuilder.Record<User>()
        (b.TableValuedSeq(tvp, "users")()).SetValue([user], command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))


    

    [<Fact>]
    let ``Record seq - custom TVP name`` () =
    
        let createConnection () = 
            createConnectionMock                         
                [
                    [ col<string> "name"; col<string> "typeName"; col<int16> "max_length"; col<int16> "precision"; col<byte> "scale"; col<byte> "is_nullable" ],
                    [
                        [ "userId"; "int"; 4s; 10uy; 0uy; 0uy ]
                        [ "name"; "nvarchar"; 20s; 0uy; 0uy; 0uy ]
                        [ "email"; "nvarchar"; 100s; 0uy; 0uy; 0uy ]
                        [ "created"; "datetime"; 8s; 0uy; 0uy; 0uy ]
                    ]                            
                ]

        let tvpBuilder = TVParamBuilder []
        let b = ParamBuilder([], createConnection, tvpBuilder)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (b.TableValuedSeq<User>("users", "USER_TVP")()).SetValue([user], command)

        Assert.Equal("USER_TVP", command.Parameters.["users"].TypeName)
