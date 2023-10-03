namespace MoreSqlFun.Core.Tests

open System
open System.Data
open Xunit
open Moq

open MoreSqlFun.Core.Builders
open MoreSqlFun.TestTools.Models

module RowTests = 

    let createDataRecordMock (fields: (string * Type * obj) list) = 
        let mock = new Mock<IDataRecord>()
        for ordinal, (name, colType, value) in fields |> List.mapi(fun i x -> i, x) do
            mock.Setup(fun x -> x.GetFieldType(ordinal)).Returns(colType) |> ignore
            mock.Setup(fun x -> x.GetName(ordinal)).Returns(name) |> ignore
            mock.Setup(fun x -> x.IsDBNull(ordinal)).Returns(Object.Equals(value, null)) |> ignore
            mock.Setup(fun x -> x.GetValue(ordinal)).Returns(value) |> ignore
            mock.Setup(fun x -> x.GetString(ordinal)).Returns<int>(fun _ -> string(value)) |> ignore
            mock.Setup(fun x -> x.GetChar(ordinal)).Returns<int>(fun _ -> Convert.ToChar(value)) |> ignore
            mock.Setup(fun x -> x.GetInt16(ordinal)).Returns<int>(fun _ -> Convert.ToInt16(value)) |> ignore
            mock.Setup(fun x -> x.GetInt32(ordinal)).Returns<int>(fun _ -> Convert.ToInt32(value)) |> ignore
            mock.Setup(fun x -> x.GetInt64(ordinal)).Returns<int>(fun _ -> Convert.ToInt64(value)) |> ignore
            mock.Setup(fun x -> x.GetDateTime(ordinal)).Returns<int>(fun _ -> Convert.ToDateTime(value)) |> ignore
            mock.Setup(fun x -> x.GetGuid(ordinal)).Returns<int>(fun _ -> value :?> Guid) |> ignore
            mock.Setup(fun x -> x.GetBoolean(ordinal)).Returns<int>(fun _ -> Convert.ToBoolean(value)) |> ignore
            mock.Setup(fun x -> x.GetDecimal(ordinal)).Returns<int>(fun _ -> Convert.ToDecimal(value)) |> ignore
            mock.Setup(fun x -> x.GetFloat(ordinal)).Returns<int>(fun _ -> Convert.ToSingle(value)) |> ignore
            mock.Setup(fun x -> x.GetDouble(ordinal)).Returns<int>(fun _ -> Convert.ToDouble(value)) |> ignore
        mock.Setup(fun x -> x.GetOrdinal(It.IsAny<string>())).Returns<string>(fun name -> fields |> List.findIndex (fun (n, _, _) -> n = name)) |> ignore
        mock.Object

    let vcol(name: string, value: 't) = 
        name, typeof<'t>, box value

    let ncol<'t>(name: string) = 
        name, typeof<'t>, null

    [<Fact>]
    let ``Simple types``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   vcol("id", 5)
                            vcol("name", "jacentino")
                            vcol("active", true)
                            vcol("created", DateTime(2023, 1, 1))
                            vcol("avatar", [| 1uy; 2uy; 3uy |])
                        ]


        let idGetter = builder.Simple<int> "id" record
        let id = idGetter.Get(record)
        let nameGetter = builder.Simple<string> "name" record
        let name = nameGetter.Get(record)
        let activeGetter = builder.Simple "active" record
        let active = activeGetter.Get(record)
        let createdGetter = builder.Simple<DateTime> "created" record
        let created = createdGetter.Get(record)
        let avaterGetter = builder.Simple<byte array>("avatar") record
        let avatar = avaterGetter.Get(record)

        Assert.Equal(5, id)
        Assert.Equal("jacentino", name)
        Assert.Equal(true, active)
        Assert.Equal(DateTime(2023, 1, 1), created)
        Assert.Equal<byte array>([| 1uy; 2uy; 3uy |], avatar)


    [<Fact>]
    let ``wrong names``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5) ]

        let ex = Assert.Throws(fun () -> builder.Simple<int> "userId" record |> ignore)
        Assert.Contains("Column doesn't exist: userId", ex.Message)


    [<Fact>]
    let ``wrong types``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5) ]

        let ex = Assert.Throws(fun () -> builder.Simple<string> "id" record |> ignore)
        Assert.Contains("Column type doesn't match field type: id (Int32 -> String)", ex.Message)


    [<Fact>]
    let ``Simple option types``() = 
        
        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   vcol("created", DateTime(2023, 1, 1)) 
                            ncol<DateTime>("updated")
                        ]

        let createdGetter = builder.Optional<DateTime> "created" record
        let created = createdGetter.Get(record)

        let updatedGetter = builder.Optional<DateTime> "updated" record
        let updated = updatedGetter.Get(record)
        
        Assert.Equal(Some (DateTime(2023, 1, 1)), created)
        Assert.Equal(None, updated)


    [<Fact>]
    let ``Option types - explicit underlying getter``() = 
        
        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   vcol("created", DateTime(2023, 1, 1)) 
                            ncol<DateTime>("updated")
                        ]

        let createdGetter = builder.Optional(builder.Simple<DateTime>("created")) record
        let created = createdGetter.Get(record)

        let updatedGetter = builder.Optional(builder.Simple<DateTime>("updated")) record
        let updated = updatedGetter.Get(record)
        
        Assert.Equal(Some (DateTime(2023, 1, 1)), created)
        Assert.Equal(None, updated)


    [<Fact>]
    let ``Char enums``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("status", 'A') ]
        
        let getter = builder.Simple<Status> "status" record
        let value = getter.Get(record)

        Assert.Equal(Status.Active, value)


    [<Fact>]
    let ``Int enums``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("role", 2) ]
        
        let getter = builder.Simple<Role> "role" record
        let value = getter.Get(record)

        Assert.Equal(Role.Regular, value)


    [<Fact>]
    let ``Attribute enums``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("access", "RW") ]
        
        let getter = builder.Simple<Access> "access" record
        let value = getter.Get(record)

        Assert.Equal(Access.ReadWrite, value)


    [<Fact>]
    let ``DateOnly converter``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("created", DateTime(2023, 1, 1)) ]
        
        let getter = builder.Simple<DateOnly> "created" record
        let value = getter.Get(record)

        Assert.Equal(DateOnly.FromDateTime(DateTime(2023, 1, 1)), value)


    [<Fact>]
    let ``TimeOnly converter``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("dayTime", TimeSpan.FromHours(5)) ]
        
        let getter = builder.Simple<TimeOnly> "dayTime" record
        let value = getter.Get(record)

        Assert.Equal(TimeOnly.FromTimeSpan(TimeSpan.FromHours(5)), value)


    [<Fact>]
    let ``Simple type tuples``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]

        let getter = builder.Tuple<int, string>("id", "name") record
        let t = getter.Get(record)

        Assert.Equal((5, "jacentino"), t)
        

    [<Fact>]
    let ``Simple type tuples - explicit item getters``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]

        let getter = builder.Tuple(builder.Simple<int>("id"), builder.Simple<string>("name")) record
        let t = getter.Get(record)

        Assert.Equal((5, "jacentino"), t)


    [<Fact>]
    let ``Tuple options - Some``() = 
        
        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]

        let getter = builder.Optional(builder.Tuple<int, string>("id", "name")) record
        let t = getter.Get(record)

        Assert.Equal(Some (5, "jacentino"), t)


    [<Fact>]
    let ``Tuple options - None``() = 
        
        let builder = RowBuilder []
        let record = createDataRecordMock [ ncol<int>("id"); ncol<string>("name") ]

        let getter = builder.Optional(builder.Tuple<int, string>("id", "name")) record
        let t = getter.Get(record)

        Assert.Equal(None, t)


    [<Fact>]
    let ``Records of simple types``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]

        let getter = builder.Record<User>() record
        let value = getter.Get(record)

        let expected = 
            {
                userId  = 5
                name    = "jacentino"
                email   = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        Assert.Equal(expected, value)
                            

    [<Fact>]
    let ``Records - prefixed names``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   vcol("user_userId", 5)
                            vcol("user_name", "jacentino")
                            vcol("user_email", "jacentino@gmail.com")
                            vcol("user_created", DateTime(2023, 1, 1))
                        ]

        let getter = builder.Record<User>("user_") record
        let value = getter.Get(record)

        let expected = 
            {
                userId  = 5
                name    = "jacentino"
                email   = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        Assert.Equal(expected, value)
                            

    [<Fact>]
    let ``Record options - Some``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]

        let getter = builder.Optional(builder.Record<User>()) record
        let value = getter.Get(record)

        let expected = 
            {
                userId  = 5
                name    = "jacentino"
                email   = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        Assert.Equal(Some expected, value)


    [<Fact>]
    let ``Record options - None``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock 
                        [   ncol<int>("userId")
                            ncol<string>("name")
                            ncol<string>("email")
                            ncol<DateTime>("created")
                        ]

        let getter = builder.Optional(builder.Record<User>()) record
        let value = getter.Get(record)

        Assert.Equal(None, value)


    [<Fact>]
    let ``Collections - list``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5) ]

        let getter = builder.Simple<int list>("") record
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Fact>]
    let ``Collections - array``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5) ]

        let x = MoreSqlFun.Core.Types.isCollectionType typeof<int array>

        let getter = builder.Simple<int array>("") record
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Fact>]
    let ``Collections - seq``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5) ]

        let getter = builder.Simple<int seq>("") record
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Fact>]
    let ``Units``() = 

        let builder = RowBuilder []
        let record = createDataRecordMock [ vcol("id", 5) ]

        let getter = builder.Simple<unit>("") record
        let t = getter.Get(record)

        Assert.Equal((), t)
