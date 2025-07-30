namespace DbFun.Core.Tests

open System
open System.Data
open Xunit
open Moq

open DbFun.Core.Builders
open DbFun.FastExpressionCompiler.Compilers
open DbFun.TestTools.Models
open DbFun.Core.Builders.RowsImpl
open DbFun.Core.Builders.Compilers

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

    let compilers: ICompiler list = [ LinqExpressionCompiler(); Compiler() ]

    let providers = 
        compilers
        |> List.map (fun compiler -> [| 
            GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(RowsImpl.getDefaultBuilders(), compiler) 
        |])
        

    [<Theory>][<MemberData(nameof providers)>]
    let ``Simple types``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("id", 5)
                            vcol("name", "jacentino")
                            vcol("active", true)
                            vcol("created", DateTime(2023, 1, 1))
                            vcol("avatar", [| 1uy; 2uy; 3uy |])
                        ]

        let builderParams = provider, record

        let idGetter = Rows.Int "id" builderParams
        let id = idGetter.Get(record)
        let nameGetter = Rows.String "name" builderParams
        let name = nameGetter.Get(record)
        let activeGetter = Rows.Bool "active" builderParams
        let active = activeGetter.Get(record)
        let createdGetter = Rows.DateTime "created" builderParams
        let created = createdGetter.Get(record)
        let avaterGetter = Rows.Auto<byte array>("avatar") builderParams
        let avatar = avaterGetter.Get(record)

        Assert.Equal(5, id)
        Assert.Equal("jacentino", name)
        Assert.Equal(true, active)
        Assert.Equal(DateTime(2023, 1, 1), created)
        Assert.Equal<byte array>([| 1uy; 2uy; 3uy |], avatar)


    [<Theory>][<MemberData(nameof providers)>]
    let ``wrong names``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider, record

        let ex = Assert.Throws(fun () -> Rows.Auto<int> "userId" builderParams |> ignore)
        Assert.Contains("Column doesn't exist: userId", ex.Message)


    [<Theory>][<MemberData(nameof providers)>]
    let ``wrong types``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider, record

        let ex = Assert.Throws(fun () -> Rows.Auto<string> "id" builderParams |> ignore)
        Assert.Contains("Column type doesn't match field type: id (Int32 -> String)", ex.Message)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Simple option types``(provider) = 
        
        let record = createDataRecordMock 
                        [   vcol("created", DateTime(2023, 1, 1)) 
                            ncol<DateTime>("updated")
                        ]
        let builderParams = provider, record

        let createdGetter = Rows.Optional<DateTime> "created" builderParams
        let created = createdGetter.Get(record)

        let updatedGetter = Rows.Optional<DateTime> "updated" builderParams
        let updated = updatedGetter.Get(record)
        
        Assert.Equal(Some (DateTime(2023, 1, 1)), created)
        Assert.Equal(None, updated)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Option types - explicit underlying getter``(provider) = 
        
        let record = createDataRecordMock 
                        [   vcol("created", DateTime(2023, 1, 1)) 
                            ncol<DateTime>("updated")
                        ]
        let builderParams = provider, record

        let createdGetter = Rows.Optional(Rows.DateTime("created")) builderParams
        let created = createdGetter.Get(record)

        let updatedGetter = Rows.Optional(Rows.DateTime("updated")) builderParams
        let updated = updatedGetter.Get(record)
        
        Assert.Equal(Some (DateTime(2023, 1, 1)), created)
        Assert.Equal(None, updated)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Char enums``(provider) = 

        let record = createDataRecordMock [ vcol("status", 'A') ]
        let builderParams = provider, record
        
        let getter = Rows.Auto<Status> "status" builderParams
        let value = getter.Get(record)

        Assert.Equal(Status.Active, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Int enums``(provider) = 

        let record = createDataRecordMock [ vcol("role", 2) ]
        let builderParams = provider, record
        
        let getter = Rows.Auto<Role> "role" builderParams
        let value = getter.Get(record)

        Assert.Equal(Role.Regular, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - no fields``(provider) = 

        let record = createDataRecordMock [ vcol("access", "RW") ]
        let builderParams = provider, record
        
        let getter = Rows.Auto<Access> "access" builderParams
        let value = getter.Get(record)

        Assert.Equal(Access.ReadWrite, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - unnamed fields``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CS"); vcol("Cash", "PLN"); ncol<string>("number"); ncol<string>("cvc") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType> "payment" builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.Cash "PLN", value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - named fields``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("Cash"); vcol("number", "1234567890"); vcol("cvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType> "payment" builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("paymentCash"); vcol("paymentnumber", "1234567890"); vcol("paymentcvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment", UnionNaming.Prefix) builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - path``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("paymentCash"); vcol("paymentnumber", "1234567890"); vcol("paymentcvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment", UnionNaming.Path) builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - case names``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("Cash"); vcol("CreditCardnumber", "1234567890"); vcol("CreditCardcvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment", UnionNaming.CaseNames) builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix and path``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("paymentCash"); vcol("paymentnumber", "1234567890"); vcol("paymentcvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment", UnionNaming.Prefix ||| UnionNaming.Path) builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix and case names``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("paymentCash"); vcol("paymentCreditCardnumber", "1234567890"); vcol("paymentCreditCardcvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment", UnionNaming.Prefix ||| UnionNaming.CaseNames) builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - path and case names``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("paymentCash"); vcol("paymentCreditCardnumber", "1234567890"); vcol("paymentCreditCardcvc", "222") ]
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment", UnionNaming.Path ||| UnionNaming.CaseNames) builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Discriminated unions - path and case names by configurator``(provider) = 

        let record = createDataRecordMock [ vcol("payment", "CC"); ncol<string>("paymentCash"); vcol("paymentCreditCardnumber", "1234567890"); vcol("paymentCreditCardcvc", "222") ]
        let provider: IRowGetterProvider = 
            RowsImpl.BaseGetterProvider(
                RowsImpl.Configurator<string * UnionNaming>((fun prefix -> prefix, UnionNaming.Path ||| UnionNaming.CaseNames), fun t -> t = typeof<PaymentType>) ::  
                RowsImpl.getDefaultBuilders(),
                Compiler())
        let builderParams = provider, record
        
        let getter = Rows.Union<PaymentType>("payment") builderParams
        let value = getter.Get(record)

        Assert.Equal(PaymentType.CreditCard("1234567890", "222"), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``DateOnly converter``(provider) = 

        let record = createDataRecordMock [ vcol("created", DateTime(2023, 1, 1)) ]
        let builderParams = provider, record
        
        let getter = Rows.Auto<DateOnly> "created" builderParams
        let value = getter.Get(record)

        Assert.Equal(DateOnly.FromDateTime(DateTime(2023, 1, 1)), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``TimeOnly converter``(provider) = 

        let record = createDataRecordMock [ vcol("dayTime", TimeSpan.FromHours(5)) ]
        let builderParams = provider, record
        
        let getter = Rows.Auto<TimeOnly> "dayTime" builderParams
        let value = getter.Get(record)

        Assert.Equal(TimeOnly.FromTimeSpan(TimeSpan.FromHours(5)), value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Simple type tuples``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]
        let builderParams = provider, record

        let getter = Rows.Tuple<int, string>("id", "name") builderParams
        let t = getter.Get(record)

        Assert.Equal((5, "jacentino"), t)
        

    [<Theory>][<MemberData(nameof providers)>]
    let ``Simple type tuples - explicit item getters``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]
        let builderParams = provider, record

        let getter = Rows.Tuple(Rows.Auto<int>("id"), Rows.Auto<string>("name")) builderParams
        let t = getter.Get(record)

        Assert.Equal((5, "jacentino"), t)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Tuple options - Some``(provider) = 
        
        let record = createDataRecordMock [ vcol("id", 5); vcol("name", "jacentino") ]
        let builderParams = provider, record

        let getter = Rows.Optional(Rows.Tuple<int, string>("id", "name")) builderParams
        let t = getter.Get(record)

        Assert.Equal(Some (5, "jacentino"), t)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Tuple options - None``(provider) = 
        
        let record = createDataRecordMock [ ncol<int>("id"); ncol<string>("name") ]
        let builderParams = provider, record

        let getter = Rows.Optional(Rows.Tuple<int, string>("id", "name")) builderParams
        let t = getter.Get(record)

        Assert.Equal(None, t)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Records of simple types``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider, record

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
                            

    [<Theory>][<MemberData(nameof providers)>]
    let ``Records - prefixed names``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("user_userId", 5)
                            vcol("user_name", "jacentino")
                            vcol("user_email", "jacentino@gmail.com")
                            vcol("user_created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider, record

        let getter = Rows.Record<User>("user_", RecordNaming.Prefix) builderParams
        let value = getter.Get(record)

        let expected = 
            {
                userId  = 5
                name    = "jacentino"
                email   = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        Assert.Equal(expected, value)
                            

    [<Theory>][<MemberData(nameof providers)>]
    let ``Records - overrides``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("id", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider, record

        let u = any<User>
        let getter = Rows.Record<User>(overrides = [RowOverride<int>(u.userId, Rows.Auto<int>("id"))]) builderParams
        let value = getter.Get(record)

        let expected = 
            {
                userId  = 5
                name    = "jacentino"
                email   = "jacentino@gmail.com"
                created = DateTime(2023, 1, 1)
            }
        Assert.Equal(expected, value)
                            

    [<Theory>][<MemberData(nameof providers)>]
    let ``Records prefix by configurator``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("user_userId", 5)
                            vcol("user_name", "jacentino")
                            vcol("user_email", "jacentino@gmail.com")
                            vcol("user_created", DateTime(2023, 1, 1))
                        ]

        let provider: IRowGetterProvider = 
            RowsImpl.BaseGetterProvider(
                RowsImpl.Configurator<string * RecordNaming>((fun prefix -> prefix, RecordNaming.Prefix), fun t -> t = typeof<User>) ::  
                RowsImpl.getDefaultBuilders(),
                Compiler())
        let builderParams = provider, record

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
                            

    [<Theory>][<MemberData(nameof providers)>]
    let ``Record options - Some``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let builderParams = provider, record

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


    [<Theory>][<MemberData(nameof providers)>]
    let ``Record options - None``(provider) = 

        let record = createDataRecordMock 
                        [   ncol<int>("userId")
                            ncol<string>("name")
                            ncol<string>("email")
                            ncol<DateTime>("created")
                        ]
        let builderParams = provider, record

        let getter = Rows.Optional(Rows.Record<User>()) builderParams
        let value = getter.Get(record)

        Assert.Equal(None, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Hierarchical records``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("userId", "jacentino")
                            vcol("password", "******")
                            vcol("createdAt", DateTime(2023, 1, 1))
                            vcol("createdBy", "admin")
                            vcol("updatedAt", DateTime(2023, 1, 1))
                            vcol("updatedBy", "admin")
                        ]
        let builderParams = provider, record

        let getter = Rows.Record<Account>() builderParams
        let value = getter.Get(record)

        let expected = 
            {
                userId      = "jacentino"
                password    = "******"
                signature   = { createdAt = DateTime(2023, 1, 1); createdBy = "admin"; updatedAt = DateTime(2023, 1, 1); updatedBy = "admin" }
            }
        Assert.Equal(expected, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Hierarchical records - prefixes``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("account_userId", "jacentino")
                            vcol("account_password", "******")
                            vcol("account_createdAt", DateTime(2023, 1, 1))
                            vcol("account_createdBy", "admin")
                            vcol("account_updatedAt", DateTime(2023, 1, 1))
                            vcol("account_updatedBy", "admin")
                        ]
        let builderParams = provider, record

        let getter = Rows.Record<Account>("account_", RecordNaming.Prefix) builderParams
        let value = getter.Get(record)

        let expected = 
            {
                userId      = "jacentino"
                password    = "******"
                signature   = { createdAt = DateTime(2023, 1, 1); createdBy = "admin"; updatedAt = DateTime(2023, 1, 1); updatedBy = "admin" }
            }
        Assert.Equal(expected, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Hierarchical records - paths``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("account_userId", "jacentino")
                            vcol("account_password", "******")
                            vcol("account_signaturecreatedAt", DateTime(2023, 1, 1))
                            vcol("account_signaturecreatedBy", "admin")
                            vcol("account_signatureupdatedAt", DateTime(2023, 1, 1))
                            vcol("account_signatureupdatedBy", "admin")
                        ]
        let builderParams = provider, record

        let getter = Rows.Record<Account>("account_", RecordNaming.Path) builderParams
        let value = getter.Get(record)

        let expected = 
            {
                userId      = "jacentino"
                password    = "******"
                signature   = { createdAt = DateTime(2023, 1, 1); createdBy = "admin"; updatedAt = DateTime(2023, 1, 1); updatedBy = "admin" }
            }
        Assert.Equal(expected, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Hierarchical records - overrides``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("userId", "jacentino")
                            vcol("password", "******")
                            vcol("createdAt", DateTime(2023, 1, 1))
                            vcol("createdBy", "admin")
                            vcol("modifiedAt", DateTime(2023, 1, 1))
                            vcol("modifiedBy", "admin")
                        ]
        let builderParams = provider, record

        let a = any<Account>
        let ovUpdatedAt = RowOverride(a.signature.updatedAt, Rows.Auto("modifiedAt"))
        let ovUpdatedBy = RowOverride(a.signature.updatedBy, Rows.Auto("modifiedBy"))
        let getter = Rows.Record<Account>(overrides = [ovUpdatedAt; ovUpdatedBy]) builderParams
        let value = getter.Get(record)

        let expected = 
            {
                userId      = "jacentino"
                password    = "******"
                signature   = { createdAt = DateTime(2023, 1, 1); createdBy = "admin"; updatedAt = DateTime(2023, 1, 1); updatedBy = "admin" }
            }
        Assert.Equal(expected, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Collections - list``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider, record

        let getter = Rows.Auto<int list>("") builderParams
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Collections - array``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider, record

        let getter = Rows.Auto<int array>("") builderParams
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Collections - seq``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider, record

        let getter = Rows.Auto<int seq>("") builderParams
        let t = getter.Get(record)

        Assert.Empty(t)


    [<Theory>][<MemberData(nameof providers)>]
    let ``Units``(provider) = 

        let record = createDataRecordMock [ vcol("id", 5) ]
        let builderParams = provider, record

        let getter = Rows.Auto<unit>("") builderParams
        let t = getter.Get(record)

        Assert.Equal((), t)



    [<Theory>][<MemberData(nameof providers)>]
    let ``No prototype calls - char enums``(provider) = 

        let record = createDataRecordMock [ vcol("status", 'A') ]
        let provider: IRowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>( NoPrototypeColumnBuilder() :: RowsImpl.getDefaultBuilders(), Compiler())
        let builderParams = provider, record
        
        let getter = Rows.Auto<Status> "status" builderParams
        let value = getter.Get(record)

        Assert.Equal(Status.Active, value)


    [<Theory>][<MemberData(nameof providers)>]
    let ``No prototype calls - records``(provider) = 

        let record = createDataRecordMock 
                        [   vcol("userId", 5)
                            vcol("name", "jacentino")
                            vcol("email", "jacentino@gmail.com")
                            vcol("created", DateTime(2023, 1, 1))
                        ]
        let provider: IRowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>( NoPrototypeColumnBuilder() :: RowsImpl.getDefaultBuilders(), Compiler())
        let builderParams = provider, record

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
