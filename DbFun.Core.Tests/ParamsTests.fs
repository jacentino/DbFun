namespace DbFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open DbFun.Core.Builders
open DbFun.FastExpressionCompiler.Compilers
open DbFun.TestTools.Models
open DbFun.Core.Builders.GenericSetters
open System.Data
open DbFun.Core.Models
open DbFun.Core.Builders.Compilers

type TestUnion = 
    | [<UnionCaseTag("EC")>] EmptyCase
    | [<UnionCaseTag("SV")>] SimpleValue of int
    | [<UnionCaseTag("TU")>] Tuple of int * string
    | [<UnionCaseTag("NI")>] NamedItems of Id: int * Name: string


module ParamsTests = 

    let connection = new SqlConnection()

    let compilers: ICompiler list = [ LinqExpressionCompiler(); Compiler()]

    let providers = 
        compilers 
        |> List.map (fun compiler -> [|
            BaseSetterProvider<IDbConnection, IDbCommand>(ParamsImpl.SequenceIndexingBuilder() :: ParamsImpl.getDefaultBuilders(), compiler)
        |])


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Simple types`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =

        use command = connection.CreateCommand()

        (Params.Auto<int>("userId")(provider, connection)).SetValue(1, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Unit`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()

        (Params.Auto<unit>("userId")(provider, connection)).SetValue((), None, command)

        Assert.Equal(0, command.Parameters.Count)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Collections - list`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Auto<int list>("userId")(provider, connection)).SetValue([ 5; 6; 7 ], None, command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Collections - array`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        let b = Params()
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Auto<int array>("userId")(provider, connection)).SetValue([| 5; 6; 7 |], None, command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Convertible collections - array`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Auto<DateOnly array>("created")(provider, connection)).SetValue([| DateOnly.FromDateTime(DateTime.Today) |], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Convertible collections - list`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Auto<DateOnly list>("created")(provider, connection)).SetValue([DateOnly.FromDateTime(DateTime.Today)], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Convertible collections - seq`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Auto<DateOnly seq>("created")(provider, connection)).SetValue(Seq.singleton (DateOnly.FromDateTime(DateTime.Today)), None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Enum collections - array`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Auto<Status array>("status")(provider, connection)).SetValue([| Status.Active |], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Enum collections - list`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Auto<Status list>("status")(provider, connection)).SetValue([ Status.Active ], None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Enum collections - seq`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Auto<Status seq>("status")(provider, connection)).SetValue(Seq.singleton Status.Active, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated union collections - seq`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from Role where access in (@access)"

        (Params.Auto<Access seq>("access")(provider, connection)).SetValue(Seq.singleton Access.ReadWrite, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@access0)", command.CommandText)
        Assert.Equal(box "RW", command.Parameters["access0"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Collections of records`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (Params.Auto<User seq>("users")(provider, connection)).SetValue([], None, command)

        Assert.Equal(0, command.Parameters.Count)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Collections of tuples`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
    
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (Params.Auto<(int * string * string * DateTime) seq>("users")(provider, connection)).SetValue([], None, command)

        Assert.Equal(0, command.Parameters.Count)
    

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples of simple types`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, string>("userId", "name")(provider, connection)).SetValue((1, "jacenty"), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of simple types - Some`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional<int>("userId")(provider, connection)).SetValue(Some 1, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  1, command.Parameters.["userId"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of simple types - None`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional<int>("userId")(provider, connection)).SetValue(None, None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  DBNull.Value, command.Parameters.["userId"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of tuples of simple types - Some`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Tuple<int, string>("userId", "name"))(provider, connection)).SetValue(Some (1, "jacenty"), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of tuples of simple types - None`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Tuple<int, string>("userId", "name"))(provider, connection)).SetValue(None, None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples of options of simple types - Some`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(provider, connection)).SetValue((Some 1, Some "jacenty"), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples of options of simple types - None`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(provider, connection)).SetValue((None, None), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples of options of simple types - Some/None`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(provider, connection)).SetValue((Some 1, None), None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Record<User>()(provider, connection)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records - name prefixes`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Record<User>("user_", RecordNaming.Prefix)(provider, connection)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user_userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user_name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user_email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user_created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records - overrides`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()
        let u = any<User>

        let setter = Params.Record<User>(overrides = [ParamOverride<int>(u.userId, Params.Auto<int>("id"))])(provider, connection)

        setter.SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["id"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat records - name prefixes by configurator`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()
        let provider: IParamSetterProvider = 
            ParamsImpl.BaseSetterProvider(
                ParamsImpl.Configurator<string * RecordNaming>((fun prefix -> prefix, RecordNaming.Prefix), fun t -> t = typeof<User>) ::  
                ParamsImpl.getDefaultBuilders(), Compiler())
        let builderParams = provider, connection :> IDbConnection

        (Params.Record<User>("user_")(provider, connection)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user_userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user_name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user_email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user_created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of flat records - Some`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Record<User>())(provider, connection)).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of flat records - None`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Record<User>())(provider, connection)).SetValue(None, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of flat records - implicit underlying assigner - Some`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional<User>("user")(provider, connection)).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Options of flat records - implicit underlying assigner - None`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Optional<User>("user")(provider, connection)).SetValue(None, None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples of flat records`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Int("orgId"), Params.Record<User>())(provider, connection)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), None, command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples of flat records - implicit underlying assigner`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, User>("orgId", "user")(provider, connection)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), None, command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Hierarchical records`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        (Params.Record<Account>()(provider, connection)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["userId"].Value)
        Assert.Equal(box "******", command.Parameters.["password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["updatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["updatedBy"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Hierarchical records - name prefixes`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        (Params.Record<Account>("account_", RecordNaming.Prefix)(provider, connection)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["account_userId"].Value)
        Assert.Equal(box "******", command.Parameters.["account_password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_updatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_updatedBy"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Hierarchical records - name paths`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let account = 
            { 
                userId = "jacenty"; 
                password = "******"; 
                signature = { createdAt = DateTime.Today; createdBy = "admin"; updatedAt = DateTime.Today; updatedBy = "admin" } 
            }
        (Params.Record<Account>("account_", RecordNaming.Path)(provider, connection)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["account_userId"].Value)
        Assert.Equal(box "******", command.Parameters.["account_password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_signaturecreatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_signaturecreatedBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_signatureupdatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_signatureupdatedBy"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Hierarchical records - overrides`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

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
        (Params.Record<Account>(overrides = [ovUpdatedAt; ovUpdatedBy])(provider, connection)).SetValue(account, None, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["userId"].Value)
        Assert.Equal(box "******", command.Parameters.["password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["modifiedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["modifiedBy"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat record sequence`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let values = seq {
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime.Today.AddDays(-1) }
        }

        (Params.Seq<User>()(provider, connection)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId0"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name0"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email0"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created0"].Value)
        Assert.Equal(box 2, command.Parameters.["userId1"].Value)
        Assert.Equal(box "placenty", command.Parameters.["name1"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["email1"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["created1"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat record list`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let values = [
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime.Today.AddDays(-1) }
        ]

        (Params.List<User>()(provider, connection)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId0"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name0"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email0"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created0"].Value)
        Assert.Equal(box 2, command.Parameters.["userId1"].Value)
        Assert.Equal(box "placenty", command.Parameters.["name1"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["email1"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["created1"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Flat record array`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let values = [|
            { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }
            { userId = 2; name = "placenty"; email = "placenty@gmail.com"; created = DateTime.Today.AddDays(-1) }
        |]

        (Params.Array<User>()(provider, connection)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId0"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name0"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email0"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created0"].Value)
        Assert.Equal(box 2, command.Parameters.["userId1"].Value)
        Assert.Equal(box "placenty", command.Parameters.["name1"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["email1"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["created1"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuple sequence`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let values = seq {
            1, "jacenty", "jacenty@gmail.com", DateTime.Today 
            2, "placenty", "placenty@gmail.com", DateTime.Today.AddDays(-1) 
        }

        (Params.Seq<int * string * string * DateTime>("user")(provider, connection)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user10"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user20"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user30"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user40"].Value)
        Assert.Equal(box 2, command.Parameters.["user11"].Value)
        Assert.Equal(box "placenty", command.Parameters.["user21"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["user31"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["user41"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuple list`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let values = [
            1, "jacenty", "jacenty@gmail.com", DateTime.Today 
            2, "placenty", "placenty@gmail.com", DateTime.Today.AddDays(-1) 
        ]

        (Params.List<int * string * string * DateTime>("user")(provider, connection)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user10"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user20"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user30"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user40"].Value)
        Assert.Equal(box 2, command.Parameters.["user11"].Value)
        Assert.Equal(box "placenty", command.Parameters.["user21"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["user31"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["user41"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuple array`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        let values = [|
            1, "jacenty", "jacenty@gmail.com", DateTime.Today 
            2, "placenty", "placenty@gmail.com", DateTime.Today.AddDays(-1) 
        |]

        (Params.Array<int * string * string * DateTime>("user")(provider, connection)).SetValue(values, None, command)

        Assert.Equal(8, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user10"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user20"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user30"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user40"].Value)
        Assert.Equal(box 2, command.Parameters.["user11"].Value)
        Assert.Equal(box "placenty", command.Parameters.["user21"].Value)
        Assert.Equal(box "placenty@gmail.com", command.Parameters.["user31"].Value)
        Assert.Equal(box (DateTime.Today.AddDays(-1)), command.Parameters.["user41"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Converters`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Auto<DateOnly>("date")(provider, connection)).SetValue(DateOnly.FromDateTime(DateTime.Today), None, command)

        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Tuples & Converters`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, DateOnly>("id", "date")(provider, connection)).SetValue((1, DateOnly.FromDateTime(DateTime.Today)), None, command)

        Assert.Equal(box 1, command.Parameters["id"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Char enums`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Auto<Status>("status")(provider, connection)).SetValue(Status.Active, None, command)

        Assert.Equal(box 'A', command.Parameters["status"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Int enums`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Auto<Role>("role")(provider, connection)).SetValue(Role.Admin, None, command)

        Assert.Equal(box 3, command.Parameters["role"].Value)


    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - simple`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Auto<Access>("access")(provider, connection)).SetValue(Access.Read, None, command)

        Assert.Equal(box "RD", command.Parameters["access"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - unnamed fields`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment")(provider, connection)).SetValue(PaymentType.Cash "PLN", None, command)

        Assert.Equal(box "CS", command.Parameters["payment"].Value)
        Assert.Equal(box "PLN", command.Parameters["Cash"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["number"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["cvc"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - named fields`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment")(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["number"].Value)
        Assert.Equal(box "222", command.Parameters["cvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["Cash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.Prefix)(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - path`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.Path)(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - case names`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.CaseNames)(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["CreditCardnumber"].Value)
        Assert.Equal(box "222", command.Parameters["CreditCardcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["Cash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix and case names`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.CaseNames ||| UnionNaming.Prefix)(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentCreditCardnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentCreditCardcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - path and case names`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.CaseNames ||| UnionNaming.Path)(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentCreditCardnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentCreditCardcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix and path`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment", UnionNaming.Prefix ||| UnionNaming.Path)(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)

    [<Theory>]
    [<MemberData(nameof providers)>]
    let ``Discriminated unions - prefix by discriminator`` (provider: BaseSetterProvider<IDbConnection, IDbCommand>) =
        use command = connection.CreateCommand()

        let provider: IParamSetterProvider = 
            ParamsImpl.BaseSetterProvider(
                ParamsImpl.Configurator<string * UnionNaming>((fun prefix -> prefix, UnionNaming.Prefix), fun t -> t = typeof<PaymentType>) ::  
                ParamsImpl.getDefaultBuilders(), 
                Compiler())
        let builderParams = provider, connection :> IDbConnection
        (Params.Union<PaymentType>("payment")(provider, connection)).SetValue(PaymentType.CreditCard ("1234567890", "222"), None, command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["paymentnumber"].Value)
        Assert.Equal(box "222", command.Parameters["paymentcvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["paymentCash"].Value)
