namespace DbFun.MsSql.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open DbFun.Core.Builders
open DbFun.MsSql.Builders
open DbFun.TestTools.Models
open DbFun.TestTools.Mocks
open Microsoft.Data.SqlClient.Server
open DbFun.Core.Builders.GenericSetters
open System.Data

module ParamsTests = 

    let connection = new SqlConnection()

    let makeBuilderParams(createConnection: unit -> IDbConnection) = 
        let provider = BaseSetterProvider<unit, IDbCommand>(ParamsImpl.getDefaultBuilders(createConnection))
        provider :> IParamSetterProvider, ()


    [<Fact>]
    let ``Record seq - explicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.TableValuedSeq<User>("users")(builderParams)).SetValue([user], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record seq - implicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.Auto<User seq>("users")(builderParams)).SetValue([user], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record list - explicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.TableValuedList<User>("users")(builderParams)).SetValue([user], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record list - implicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.Auto<User list>("users")(builderParams)).SetValue([user], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record array - explicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.TableValuedArray<User>("users")(builderParams)).SetValue([| user |], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record array - implicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.Auto<User array>("users")(builderParams)).SetValue([| user |], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))
    

    [<Fact>]
    let ``Record seq - super explicit`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        let tvp = TVParams.Record<User>()
        (Params.TableValuedSeq(tvp, "users")(builderParams)).SetValue([user], None, command)

        let record = command.Parameters.["users"].Value :?> SqlDataRecord seq |> Seq.head
        Assert.Equal(3, record.GetInt32(0))
        Assert.Equal("jacentino", record.GetString(1))
        Assert.Equal("jacentino@gmail.com", record.GetString(2))
        Assert.Equal(DateTime(2023, 1, 1), record.GetDateTime(3))


    

    [<Fact>]
    let ``Record seq - custom TVP name`` () =
    
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

        let builderParams = makeBuilderParams(createConnection)
        use command = connection.CreateCommand()

        let user = 
            {
                userId = 3
                name = "jacentino"
                email = "jacentino@gmail.com" 
                created = DateTime(2023, 1, 1)
            }
        (Params.TableValuedSeq<User>("users", "USER_TVP")(builderParams)).SetValue([user], None, command)

        Assert.Equal("USER_TVP", command.Parameters.["users"].TypeName)
