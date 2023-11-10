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
    let provider = BaseSetterProvider<unit, IDbCommand>(ParamsImpl.getDefaultBuilders())
    let builderParams = provider :> IParamSetterProvider, ()

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

        (Params.Simple<int>("userId")(builderParams)).SetValue(1, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Unit`` () =
    
        use command = connection.CreateCommand()

        (Params.Simple<unit>("userId")(builderParams)).SetValue((), command)

        Assert.Equal(0, command.Parameters.Count)


    [<Fact>]
    let ``Collections - list`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Simple<int list>("userId")(builderParams)).SetValue([ 5; 6; 7 ], command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)

    [<Fact>]
    let ``Collections - array`` () =
    
        let b = Params()
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Simple<int array>("userId")(builderParams)).SetValue([| 5; 6; 7 |], command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)


    [<Fact>]
    let ``Convertible collections - array`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Simple<DateOnly array>("created")(builderParams)).SetValue([| DateOnly.FromDateTime(DateTime.Today) |], command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)

    [<Fact>]
    let ``Convertible collections - list`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Simple<DateOnly list>("created")(builderParams)).SetValue([DateOnly.FromDateTime(DateTime.Today)], command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)


    [<Fact>]
    let ``Convertible collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where created in (@created)"

        (Params.Simple<DateOnly seq>("created")(builderParams)).SetValue(Seq.singleton (DateOnly.FromDateTime(DateTime.Today)), command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@created0)", command.CommandText)
        Assert.Equal(box DateTime.Today, command.Parameters["created0"].Value)


    [<Fact>]
    let ``Enum collections - array`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Simple<Status array>("status")(builderParams)).SetValue([| Status.Active |], command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Fact>]
    let ``Enum collections - list`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Simple<Status list>("status")(builderParams)).SetValue([ Status.Active ], command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Fact>]
    let ``Enum collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where status in (@status)"

        (Params.Simple<Status seq>("status")(builderParams)).SetValue(Seq.singleton Status.Active, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@status0)", command.CommandText)
        Assert.Equal(box 'A', command.Parameters["status0"].Value)


    [<Fact>]
    let ``Discriminated union collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from Role where access in (@access)"

        (Params.Simple<Access seq>("access")(builderParams)).SetValue(Seq.singleton Access.ReadWrite, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@access0)", command.CommandText)
        Assert.Equal(box "RW", command.Parameters["access0"].Value)


    [<Fact>]
    let ``Collections of records`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (Params.Simple<User seq>("users")(builderParams)).SetValue([], command)

        Assert.Equal(0, command.Parameters.Count)


    [<Fact>]
    let ``Collections of tuples`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (Params.Simple<(int * string * string * DateTime) seq>("users")(builderParams)).SetValue([], command)

        Assert.Equal(0, command.Parameters.Count)
    

    [<Fact>]
    let ``Tuples of simple types``() = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, string>("userId", "name")(builderParams)).SetValue((1, "jacenty"), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Options of simple types - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional<int>("userId")(builderParams)).SetValue(Some 1, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  1, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Options of simple types - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional<int>("userId")(builderParams)).SetValue(None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  DBNull.Value, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Options of tuples of simple types - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Tuple<int, string>("userId", "name"))(builderParams)).SetValue(Some (1, "jacenty"), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Options of tuples of simple types - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Tuple<int, string>("userId", "name"))(builderParams)).SetValue(None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - Some``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(builderParams)).SetValue((Some 1, Some "jacenty"), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - None``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(builderParams)).SetValue((None, None), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - Some/None``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Optional<int>("userId"), Params.Optional<string>("name"))(builderParams)).SetValue((Some 1, None), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Flat records``() = 

        use command = connection.CreateCommand()

        (Params.Record<User>()(builderParams)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Flat records - name prefixes``() = 

        use command = connection.CreateCommand()

        (Params.Record<User>("user_")(builderParams)).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user_userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user_name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user_email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user_created"].Value)


    [<Fact>]
    let ``Flat records - overrides``() = 

        use command = connection.CreateCommand()
        let u = any<User>

        let setter = Params.Record<User>(ParamOverride<int>(u.userId, Params.Simple<int>("id")))(builderParams)

        setter.SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["id"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Record<User>())(builderParams)).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional(Params.Record<User>())(builderParams)).SetValue(None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - implicit underlying assigner - Some``() = 

        use command = connection.CreateCommand()

        (Params.Optional<User>("user")(builderParams)).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - implicit underlying assigner - None``() = 

        use command = connection.CreateCommand()

        (Params.Optional<User>("user")(builderParams)).SetValue(None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Tuples of flat records``() = 

        use command = connection.CreateCommand()

        (Params.Tuple(Params.Int("orgId"), Params.Record<User>())(builderParams)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Tuples of flat records - implicit underlying assigner``() = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, User>("orgId", "user")(builderParams)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), command)

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
        (Params.Record<Account>()(builderParams)).SetValue(account, command)

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
        (Params.Record<Account>("account_")(builderParams)).SetValue(account, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["account_userId"].Value)
        Assert.Equal(box "******", command.Parameters.["account_password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["account_updatedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["account_updatedBy"].Value)


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
        let ovUpdatedAt = ParamOverride(a.signature.updatedAt, Params.Simple("modifiedAt"))
        let ovUpdatedBy = ParamOverride(a.signature.updatedBy, Params.Simple("modifiedBy"))
        (Params.Record<Account>(ovUpdatedAt, ovUpdatedBy)(builderParams)).SetValue(account, command)

        Assert.Equal(6, command.Parameters.Count)
        Assert.Equal(box "jacenty", command.Parameters.["userId"].Value)
        Assert.Equal(box "******", command.Parameters.["password"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["createdAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["createdBy"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["modifiedAt"].Value)
        Assert.Equal(box "admin", command.Parameters.["modifiedBy"].Value)


    [<Fact>]
    let ``Converters``() = 

        use command = connection.CreateCommand()

        (Params.Simple<DateOnly>("date")(builderParams)).SetValue(DateOnly.FromDateTime(DateTime.Today), command)

        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Fact>]
    let ``Tuples & Converters``() = 

        use command = connection.CreateCommand()

        (Params.Tuple<int, DateOnly>("id", "date")(builderParams)).SetValue((1, DateOnly.FromDateTime(DateTime.Today)), command)

        Assert.Equal(box 1, command.Parameters["id"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Fact>]
    let ``Char enums``() =
        use command = connection.CreateCommand()

        (Params.Simple<Status>("status")(builderParams)).SetValue(Status.Active, command)

        Assert.Equal(box 'A', command.Parameters["status"].Value)

    [<Fact>]
    let ``Int enums``() =
        use command = connection.CreateCommand()

        (Params.Simple<Role>("role")(builderParams)).SetValue(Role.Admin, command)

        Assert.Equal(box 3, command.Parameters["role"].Value)


    [<Fact>]
    let ``Discriminated unions - simple``() =
        use command = connection.CreateCommand()

        (Params.Simple<Access>("access")(builderParams)).SetValue(Access.Read, command)

        Assert.Equal(box "RD", command.Parameters["access"].Value)

    [<Fact>]
    let ``Discriminated unions - unnamed fields``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment")(builderParams)).SetValue(PaymentType.Cash "PLN", command)

        Assert.Equal(box "CS", command.Parameters["payment"].Value)
        Assert.Equal(box "PLN", command.Parameters["Cash"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["number"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["cvc"].Value)

    [<Fact>]
    let ``Discriminated unions - named fields``() =
        use command = connection.CreateCommand()

        (Params.Union<PaymentType>("payment")(builderParams)).SetValue(PaymentType.CreditCard ("1234567890", "222"), command)

        Assert.Equal(box "CC", command.Parameters["payment"].Value)
        Assert.Equal(box "1234567890", command.Parameters["number"].Value)
        Assert.Equal(box "222", command.Parameters["cvc"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters["Cash"].Value)
