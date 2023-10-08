namespace MoreSqlFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open MoreSqlFun.Core.Builders
open MoreSqlFun.TestTools.Models
open MoreSqlFun.Core.Builders.GenericSetters
open System.Data

module ParamsTests = 

    let connection = new SqlConnection()
    let provider = BaseSetterProvider<unit, IDbCommand>(ParamsImpl.getDefaultBuilders())
    let builderParams = provider :> IParamSetterProvider, ()

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
    let ``Collections - seq`` () =
    
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (Params.Simple<int seq>("userId")(builderParams)).SetValue(Seq.singleton 5, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@userId0)", command.CommandText)


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
        let u = Unchecked.defaultof<User>

        let setter = Params.Record<User>(ParamOverride<int>(<@ u.userId @>, Params.Simple<int>("id")))(builderParams)

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

        (Params.Tuple(Params.Simple<int>("orgId"), Params.Record<User>())(builderParams)).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), command)

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
    let ``Attribute enums``() =
        use command = connection.CreateCommand()

        (Params.Simple<Access>("access")(builderParams)).SetValue(Access.Read, command)

        Assert.Equal(box "RD", command.Parameters["access"].Value)
