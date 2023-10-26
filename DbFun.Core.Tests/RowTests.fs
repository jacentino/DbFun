namespace DbFun.Core.Tests

open System
open System.Data
open Xunit
open Moq

open DbFun.Core.Builders
open DbFun.TestTools.Models

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

    let provider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(RowsImpl.getDefaultBuilders())

    [<Fact>]
    let ``Simple types``() = 

        let record = createDataRecordMock 
                        [   vcol("id", 5)
                            vcol("name", "jacentino")
                            vcol("active", true)
                            vcol("created", DateTime(2023, 1, 1))
                            vcol("avatar", [| 1uy; 2uy; 3uy |])
                        ]

        let builderParams = provider :> IRowGetterProvider, record

        let idGetter = Rows.Int "id" builderParams
        let id = idGetter.Get(record)
        let nameGetter = Rows.String "name" builderParams
        let name = nameGetter.Get(record)
        let activeGetter = Rows.Bool "active" builderParams
        let active = activeGetter.Get(record)
        let createdGetter = Rows.DateTime "created" builderParams
        let created = createdGetter.Get(record)
        let avaterGetter = Rows.Simple<byte array>("avatar") builderParams
        let avatar = avaterGetter.Get(record)

        Assert.Equal(5, id)
        Assert.Equal("jacentino", name)
        Assert.Equal(true, active)
        Assert.Equal(DateTime(2023, 1, 1), created)
        Assert.Equal<byte array>([| 1uy; 2uy; 3uy |], avatar)


    [<Fact>]
    let ``wrong names``() = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider :> IRowGetterProvider, record

        let ex = Assert.Throws(fun () -> Rows.Simple<int> "userId" builderParams |> ignore)
        Assert.Contains("Column doesn't exist: userId", ex.Message)


    [<Fact>]
    let ``wrong types``() = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider :> IRowGetterProvider, record

        let ex = Assert.Throws(fun () -> Rows.Simple<string> "id" builderParams |> ignore)
        Assert.Contains("Column type doesn't match field type: id (Int32 -> String)", ex.Message)


    [<Fact>]
    let ``Simple option types``() = 
        
        let record = createDataRecordMock 
                        [   vcol("created", DateTime(2023, 1, 1)) 
                            ncol<DateTime>("updated")
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let createdGetter = Rows.Optional<DateTime> "created" builderParams
        let created = createdGetter.Get(record)

        let updatedGetter = Rows.Optional<DateTime> "updated" builderParams
        let updated = updatedGetter.Get(record)
        
        Assert.Equal(Some (DateTime(2023, 1, 1)), created)
        Assert.Equal(None, updated)


    [<Fact>]
    let ``Option types - explicit underlying getter``() = 
        
        let record = createDataRecordMock 
                        [   vcol("created", DateTime(2023, 1, 1)) 
                            ncol<DateTime>("updated")
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let createdGetter = Rows.Optional(Rows.DateTime("created")) builderParams
        let created = createdGetter.Get(record)

        let updatedGetter = Rows.Optional(Rows.DateTime("updated")) builderParams
        let updated = updatedGetter.Get(record)
        
        Assert.Equal(Some (DateTime(2023, 1, 1)), created)
        Assert.Equal(None, updated)


    [<Fact>]
    let ``Char enums``() = 

        let record = createDataRecordMock [ vcol("status", 'A') ]
        let builderParams = provider :> IRowGetterProvider, record
        
        let getter = Rows.Simple<Status> "status" builderParams
        let value = getter.Get(record)

        Assert.Equal(Status.Active, value)


    [<Fact>]
    let ``Int enums``() = 

        let record = createDataRecordMock [ vcol("role", 2) ]
        let builderParams = provider :> IRowGetterProvider, record
        
        let getter = Rows.Simple<Role> "role" builderParams
        let value = getter.Get(record)

        Assert.Equal(Role.Regular, value)


    [<Fact>]
    let ``Attribute enums``() = 

        let record = createDataRecordMock [ vcol("access", "RW") ]
        let builderParams = provider :> IRowGetterProvider, record
        
        let getter = Rows.Simple<Access> "access" builderParams
        let value = getter.Get(record)

        Assert.Equal(Access.ReadWrite, value)


    [<Fact>]
    let ``DateOnly converter``() = 

        let record = createDataRecordMock [ vcol("created", DateTime(2023, 1, 1)) ]
        let builderParams = provider :> IRowGetterProvider, record
        
        let getter = Rows.Simple<DateOnly> "created" builderParams
        let value = getter.Get(record)

        Assert.Equal(DateOnly.FromDateTime(DateTime(2023, 1, 1)), value)


    [<Fact>]
    let ``TimeOnly converter``() = 

        let record = createDataRecordMock [ vcol("dayTime", TimeSpan.FromHours(5)) ]
        let builderParams = provider :> IRowGetterProvider, record
        
        let getter = Rows.Simple<TimeOnly> "dayTime" builderParams
        let value = getter.Get(record)

        Assert.Equal(TimeOnly.FromTimeSpan(TimeSpan.FromHours(5)), value)


    [<Fact>]
    let ``Simple type tuples``() = 

        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Tuple<int, string>("id", "name") builderParams
        let t = getter.Get(record)

        Assert.Equal((5, "jacentino"), t)
        

    [<Fact>]
    let ``Simple type tuples - explicit item getters``() = 

        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Tuple(Rows.Simple<int>("id"), Rows.Simple<string>("name")) builderParams
        let t = getter.Get(record)

        Assert.Equal((5, "jacentino"), t)


    [<Fact>]
    let ``Tuple options - Some``() = 
        
        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Optional(Rows.Tuple<int, string>("id", "name")) builderParams
        let t = getter.Get(record)

        Assert.Equal(Some (5, "jacentino"), t)


    [<Fact>]
    let ``Tuple options - None``() = 
        
        let record = createDataRecordMock [ ncol<int>("id"); ncol<string>("name") ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Optional(Rows.Tuple<int, string>("id", "name")) builderParams
        let t = getter.Get(record)

        Assert.Equal(None, t)


    [<Fact>]
    let ``Records of simple types``() = 

        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Record<User>() builderParams
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

        let record = createDataRecordMock 
                        [   vcol("user_userId", 5)
                            vcol("user_name", "jacentino")
                            vcol("user_email", "jacentino@gmail.com")
                            vcol("user_created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Record<User>("user_") builderParams
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
    let ``Records - overrides``() = 

        let record = createDataRecordMock 
                        [   vcol("id", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let u = any<User>
        let getter = Rows.Record<User>(RowOverride<int>(u.userId, Rows.Simple<int>("id"))) builderParams
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

        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Optional(Rows.Record<User>()) builderParams
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

        let record = createDataRecordMock 
                        [   ncol<int>("userId")
                            ncol<string>("name")
                            ncol<string>("email")
                            ncol<DateTime>("created")
                        ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Optional(Rows.Record<User>()) builderParams
        let value = getter.Get(record)

        Assert.Equal(None, value)


    [<Fact>]
    let ``Collections - list``() = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Simple<int list>("") builderParams
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Fact>]
    let ``Collections - array``() = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Simple<int array>("") builderParams
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Fact>]
    let ``Collections - seq``() = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Simple<int seq>("") builderParams
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Fact>]
    let ``Units``() = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider :> IRowGetterProvider, record

        let getter = Rows.Simple<unit>("") builderParams
        let t = getter.Get(record)

        Assert.Equal((), t)
