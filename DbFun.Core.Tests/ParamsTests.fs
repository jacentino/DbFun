namespace DbFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open DbFun.Core.Builders
open DbFun.TestTools.Models
open DbFun.Core.Builders.GenericSetters
open System.Data



open DbFun.Core.Models

type TestUnion = 
    | [<UnionCaseTag("EC")>] EmptyCase
    | [<UnionCaseTag("SV")>] SimpleValue of int
    | [<UnionCaseTag("TU")>] Tuple of int * string
    | [<UnionCaseTag("NI")>] NamedItems of Id: int * Name: string


module ParamsTests = 

    let connection = new SqlConnection()
    let provider: IParamSetterProvider = BaseSetterProvider<unit, IDbCommand>(ParamsImpl.SequenceIndexingBuilder() :: ParamsImpl.getDefaultBuilders()) 
    let builderParams = provider, ()

    open FSharp.Reflection

    [<Fact>]
    let ``Explore DU``() = 
        let duType = typeof<TestUnion>
        let cases = FSharpType.GetUnionCases duType
        let props = duType.GetProperties()
        let methods = duType.GetMethods()
        let fields = cases |> Array.map (fun uc -> uc, uc.GetFields())
        let nestedTypes = cases |> Array.map (fun uc -> duType.GetNestedType(uc.Name))

        let isUnionCase = FSharpType.IsUnion nestedTypes.[1].BaseType

        Assert.NotEmpty(cases)
        Assert.NotEmpty(props)

    [<Fact>]
    let ``Simple types`` () =
    
        use command = connection.CreateCommand()

        (Params.Auto<int>("userId")(builderParams)).SetValue(1, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Unit`` () =
    
        use command = connection.CreateCommand()

        (Params.Auto<unit>("userId")(builderParams)).SetValue((), None, command)

        Assert.Equal(0, command.Parameters.Count)


    [<Fact>]
    let ``Collections - list`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Auto<int list>("userId")(builderParams)).SetValue([ 5; 6; 7 ], None, command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)

    [<Fact>]
    let ``Collections - array`` () =
    
        let b = Params()
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Auto<int array>("userId")(builderParams)).SetValue([| 5; 6; 7 |], None, command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)


    [<Fact>]
    let ``Convertible collections - array`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Auto<DateOnly array>("created")(builderParams)).SetValue([| DateOnly.FromDateTime(DateTime.Today) |], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)

    [<Fact>]
    let ``Convertible collections - list`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Auto<DateOnly list>("created")(builderParams)).SetValue([DateOnly.FromDateTime(DateTime.Today)], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)


    [<Fact>]
    let ``Convertible collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Auto<DateOnly seq>("created")(builderParams)).SetValue(Seq.singleton (DateOnly.FromDateTime(DateTime.Today)), None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)


    [<Fact>]
    let ``Enum collections - array`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Auto<Status array>("status")(builderParams)).SetValue([| Status.Active |], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Fact>]
    let ``Enum collections - list`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Auto<Status list>("status")(builderParams)).SetValue([ Status.Active ], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Fact>]
    let ``Enum collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Auto<Status seq>("status")(builderParams)).SetValue(Seq.singleton Status.Active, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Fact>]
    let ``Discriminated union collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from Role where access in (@access)"

        (Params.Auto<Access seq>("access")(builderParams)).SetValue(Seq.singleton Access.ReadWrite, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@access0)", command.CommandText)
        Assert.Equal(box "RW", command.Parameters["access0"].Value)


    [<Fact>]
    let ``Collections of records`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (Params.Auto<User seq>("users")(builderParams)).SetValue([], None, command)

        Assert.Equal(0, command.Parameters.Count)


    [<Fact>]
    let ``Collections of tuples`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (Params.Auto<(int * string * string * DateTime) seq>("users")(builderParams)).SetValue([], None, command)

        Assert.Equal(0, command.Parameters.Count)
    

    [<Fact>]
    let ``Tuples of simple types``() = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, string>("userId", "name")(builderParams)).SetValue((1, "jacenty"), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Options of simple types - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional<int>("userId")(builderParams)).SetValue(Some 1, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  1, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Options of simple types - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional<int>("userId")(builderParams)).SetValue(None, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  DBNull.Value, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Options of tuples of simple types - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Tuple<int, string>("userId", "name"))(builderParams)).SetValue(Some (1, "jacenty"), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Options of tuples of simple types - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Tuple<int, string>("userId", "name"))(builderParams)).SetValue(None, None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - Some``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(builderParams)).SetValue((Some 1, Some "jacenty"), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - None``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(builderParams)).SetValue((None, None), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - Some/None``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(builderParams)).SetValue((Some 1, None), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Flat records``() = 

        use command = connection.CreateCommand()

        (Params.Record<User>()(builderParams)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Flat records - name prefixes``() = 

        use command = connection.CreateCommand()

        (Params.Record<User>("user_", RecordNaming.Prefix)(builderParams)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user_userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user_name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user_email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user_created"].Value)


    [<Fact>]
    let ``Flat records - overrides``() = 

        use command = connection.CreateCommand()
        let u = any<User>

        let setter = Params.Record<User>(overrides = [ParamOverride<int>(u.userId, Params.Auto<int>("id"))])(builderParams)

        setter.SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["id"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Flat records - name prefixes by configurator``() = 

        use command = connection.CreateCommand()
        let provider: IParamSetterProvider = 
            ParamsImpl.BaseSetterProvider(
                ParamsImpl.Configurator<string * RecordNaming>((fun prefix -> prefix, RecordNaming.Prefix), fun t -> t = typeof<User>) ::  
                ParamsImpl.getDefaultBuilders())
        let builderParams = provider, ()

        (Params.Record<User>("user_")(builderParams)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user_userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user_name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user_email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user_created"].Value)


    [<Fact>]
    let ``Options of flat records - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Record<User>())(builderParams)).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Record<User>())(builderParams)).SetValue(None, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - implicit underlying assigner - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional<User>("user")(builderParams)).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - implicit underlying assigner - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional<User>("user")(builderParams)).SetValue(None, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Tuples of flat records``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Int("orgId"), Params.Record<User>())(builderParams)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), None, command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Tuples of flat records - implicit underlying assigner``() = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, User>("orgId", "user")(builderParams)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), None, command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Hierarchical records``() = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        (Params.Record<Account>()(builderParams)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["userId"].Value)
        Assert.Equal(box "******", command.Parameters.["password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["updatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["updatedBy"].Value)


    [<Fact>]
    let ``Hierarchical records - name prefixes``() = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        (Params.Record<Account>("account_", RecordNaming.Prefix)(builderParams)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["account_userId"].Value)
        Assert.Equal(box "******", command.Parameters.["account_password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_updatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_updatedBy"].Value)


    [<Fact>]
    let ``Hierarchical records - name paths``() = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        (Params.Record<Account>("account_", RecordNaming.Path)(builderParams)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["account_userId"].Value)
        Assert.Equal(box "******", command.Parameters.["account_password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_signaturecreatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_signaturecreatedBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_signatureupdatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_signatureupdatedBy"].Value)


    [<Fact>]
    let ``Hierarchical records - overrides``() = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        let a = any<Account>
        let ovUpdatedAt = ParamOverride(a.signature.updatedAt, Params.Auto("modifiedAt"))
        let ovUpdatedBy = ParamOverride(a.signature.updatedBy, Params.Auto("modifiedBy"))
        (Params.Record<Account>(overrides = [ovUpdatedAt; ovUpdatedBy])(builderParams)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["userId"].Value)
        Assert.Equal(box "******", command.Parameters.["password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["modifiedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["modifiedBy"].Value)


    [<Fact>]
    let ``Flat record sequence``() = 

        use command = connection.CreateCommand()

        let values = seq {
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime.Today.AddDays(-1) }
        }

        (Params.Seq<User>()(builderParams)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId0"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name0"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email0"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created0"].Value)
        Assert.Equal(box 2, command.Parameters.["userId1"].Value)
        Assert.Equal(box "placenty", command.Parameters.["name1"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["email1"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["created1"].Value)


    [<Fact>]
    let ``Flat record list``() = 

        use command = connection.CreateCommand()

        let values = [
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime.Today.AddDays(-1) }
        ]

        (Params.List<User>()(builderParams)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId0"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name0"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email0"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created0"].Value)
        Assert.Equal(box 2, command.Parameters.["userId1"].Value)
        Assert.Equal(box "placenty", command.Parameters.["name1"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["email1"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["created1"].Value)


    [<Fact>]
    let ``Flat record array``() = 

        use command = connection.CreateCommand()

        let values = [|
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime.Today.AddDays(-1) }
        |]

        (Params.Array<User>()(builderParams)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId0"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name0"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email0"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created0"].Value)
        Assert.Equal(box 2, command.Parameters.["userId1"].Value)
        Assert.Equal(box "placenty", command.Parameters.["name1"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["email1"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["created1"].Value)


    [<Fact>]
    let ``Tuple sequence``() = 

        use command = connection.CreateCommand()

        let values = seq {
            1, "jacenty", "jacenty@gmail.com", DateTime.Today 
            2, "placenty", "placenty@gmail.com", DateTime.Today.AddDays(-1) 
        }

        (Params.Seq<int * string * string * DateTime>("user")(builderParams)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user10"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user20"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user30"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user40"].Value)
        Assert.Equal(box 2, command.Parameters.["user11"].Value)
        Assert.Equal(box "placenty", command.Parameters.["user21"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["user31"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["user41"].Value)


    [<Fact>]
    let ``Tuple list``() = 

        use command = connection.CreateCommand()

        let values = [
            1, "jacenty", "jacenty@gmail.com", DateTime.Today 
            2, "placenty", "placenty@gmail.com", DateTime.Today.AddDays(-1) 
        ]

        (Params.List<int * string * string * DateTime>("user")(builderParams)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user10"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user20"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user30"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user40"].Value)
        Assert.Equal(box 2, command.Parameters.["user11"].Value)
        Assert.Equal(box "placenty", command.Parameters.["user21"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["user31"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["user41"].Value)


    [<Fact>]
    let ``Tuple array``() = 

        use command = connection.CreateCommand()

        let values = [|
            1, "jacenty", "jacenty@gmail.com", DateTime.Today 
            2, "placenty", "placenty@gmail.com", DateTime.Today.AddDays(-1) 
        |]

        (Params.Array<int * string * string * DateTime>("user")(builderParams)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user10"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user20"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user30"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user40"].Value)
        Assert.Equal(box 2, command.Parameters.["user11"].Value)
        Assert.Equal(box "placenty", command.Parameters.["user21"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["user31"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["user41"].Value)


    [<Fact>]
    let ``Converters``() = 

        use command = connection.CreateCommand()

        (Params.Auto<DateOnly>("date")(builderParams)).SetValue(DateOnly.FromDateTime(DateTime.Today), None, command)

        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Fact>]
    let ``Tuples & Converters``() = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, DateOnly>("id", "date")(builderParams)).SetValue((1, DateOnly.FromDateTime(DateTime.Today)), None, command)

        Assert.Equal(box 1, command.Parameters["id"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Fact>]
    let ``Char enums``() =
        use command = connection.CreateCommand()

        (Params.Auto<Status>("status")(builderParams)).SetValue(Status.Active, None, command)

        Assert.Equal(box 'A', command.Parameters["status"].Value)

    [<Fact>]
    let ``Int enums``() =
        use command = connection.CreateCommand()

        (Params.Auto<Role>("role")(builderParams)).SetValue(Role.Admin, None, command)

        Assert.Equal(box 3, command.Parameters["role"].Value)


    [<Fact>]
    let ``Discriminated unions - simple``() =
        use command = connection.CreateCommand()

        (Params.Auto<Access>("access")(builderParams)).SetValue(Access.Read, None, command)

        Assert.Equal(box "RD", command.Parameters["access"].Value)

    [<Fact>]
    let ``Discriminated unions - unnamed fields``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment")(builderParams)).SetValue(PaymentType.Cash "PLN", None, command)

        Assert.Equal(box "CS", command.Parameters["payment"].Value)
        Assert.Equal(box "PLN", command.Parameters["Cash"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["number"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["cvc"].Value)

    [<Fact>]
    let ``Discriminated unions - named fields``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment")(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["number"].Value)
        Assert.Equal(box "222", command.Parameters["cvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["Cash"].Value)

    [<Fact>]
    let ``Discriminated unions - prefix``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.Prefix)(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Fact>]
    let ``Discriminated unions - path``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.Path)(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Fact>]
    let ``Discriminated unions - case names``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.CaseNames)(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["CreditCardnumber"].Value)
        Assert.Equal(box "222", command.Parameters["CreditCardcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["Cash"].Value)

    [<Fact>]
    let ``Discriminated unions - prefix and case names``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.CaseNames ||| UnionNaming.Prefix)(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentCreditCardnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentCreditCardcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Fact>]
    let ``Discriminated unions - path and case names``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.CaseNames ||| UnionNaming.Path)(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentCreditCardnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentCreditCardcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Fact>]
    let ``Discriminated unions - prefix and path``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.Prefix ||| UnionNaming.Path)(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Fact>]
    let ``Discriminated unions - prefix by discriminator``() =
        use command = connection.CreateCommand()

        let provider: IParamSetterProvider = 
            ParamsImpl.BaseSetterProvider(
                ParamsImpl.Configurator<string * UnionNaming>((fun prefix -> prefix, UnionNaming.Prefix), fun t -> t = typeof<PaymentType>) ::  
                ParamsImpl.getDefaultBuilders())
        let builderParams = provider, ()
        (Params.Union<PaymentType>("payment")(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)
