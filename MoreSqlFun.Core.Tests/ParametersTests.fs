namespace MoreSqlFun.Core.Tests

open System
open Xunit
open Microsoft.Data.SqlClient
open MoreSqlFun.Core.Builders
open MoreSqlFun.TestTools.Models

module ParametersTests = 

    let connection = new SqlConnection()

    [<Fact>]
    let ``Simple types`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Simple<int>("userId")()).SetValue(1, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Unit`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Simple<unit>("userId")()).SetValue((), command)

        Assert.Equal(0, command.Parameters.Count)


    [<Fact>]
    let ``Collections - list`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (b.Simple<int list>("userId")()).SetValue([ 5; 6; 7 ], command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)

    [<Fact>]
    let ``Collections - array`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (b.Simple<int array>("userId")()).SetValue([| 5; 6; 7 |], command)

        Assert.Equal(3, command.Parameters.Count)
        Assert.Contains("(@userId0, @userId1, @userId2)", command.CommandText)


    [<Fact>]
    let ``Collections - seq`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()
        command.CommandText <- "select * from User where userId in (@userId)"

        (b.Simple<int seq>("userId")()).SetValue(Seq.singleton 5, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Contains("(@userId0)", command.CommandText)


    [<Fact>]
    let ``Collections of records`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (b.Simple<User seq>("users")()).SetValue([], command)

        Assert.Equal(0, command.Parameters.Count)


    [<Fact>]
    let ``Collections of tuples`` () =
    
        let b = ParamBuilder([])
        use command = connection.CreateCommand()
        command.CommandText <- "insert into User (id, name, email, created) values (@id, @name, @email, @created)"

        (b.Simple<(int * string * string * DateTime) seq>("users")()).SetValue([], command)

        Assert.Equal(0, command.Parameters.Count)
    

    [<Fact>]
    let ``Tuples of simple types``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple<int, string>("userId", "name")()).SetValue((1, "jacenty"), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Options of simple types - Some``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional<int>("userId")()).SetValue(Some 1, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  1, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Options of simple types - None``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional<int>("userId")()).SetValue(None, command)

        Assert.Equal(1, command.Parameters.Count)
        Assert.Equal(box  DBNull.Value, command.Parameters.["userId"].Value)


    [<Fact>]
    let ``Options of tuples of simple types - Some``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional(b.Tuple<int, string>("userId", "name"))()).SetValue(Some (1, "jacenty"), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Options of tuples of simple types - None``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional(b.Tuple<int, string>("userId", "name"))()).SetValue(None, command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - Some``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple(b.Optional<int>("userId"), b.Optional<string>("name"))()).SetValue((Some 1, Some "jacenty"), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - None``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple(b.Optional<int>("userId"), b.Optional<string>("name"))()).SetValue((None, None), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Tuples of options of simple types - Some/None``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple(b.Optional<int>("userId"), b.Optional<string>("name"))()).SetValue((Some 1, None), command)

        Assert.Equal(2, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)


    [<Fact>]
    let ``Flat records``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Record<User>()()).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Flat records - name prefixes``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Record<User>("user_")()).SetValue({ userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["user_userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["user_name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["user_email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["user_created"].Value)


    [<Fact>]
    let ``Options of flat records - Some``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional(b.Record<User>())()).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - None``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional(b.Record<User>())()).SetValue(None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - implicit underlying assigner - Some``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional<User>("user")()).SetValue(Some { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Options of flat records - implicit underlying assigner - None``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Optional<User>("user")()).SetValue(None, command)

        Assert.Equal(4, command.Parameters.Count)
        Assert.Equal(box DBNull.Value, command.Parameters.["userId"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["name"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["email"].Value)
        Assert.Equal(box DBNull.Value, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Tuples of flat records``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple(b.Simple<int>("orgId"), b.Record<User>())()).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Tuples of flat records - implicit underlying assigner``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple<int, User>("orgId", "user")()).SetValue((10, { userId = 1; name = "jacenty"; email = "jacenty@gmail.com"; created = DateTime.Today }), command)

        Assert.Equal(5, command.Parameters.Count)
        Assert.Equal(box 10, command.Parameters.["orgId"].Value)
        Assert.Equal(box 1, command.Parameters.["userId"].Value)
        Assert.Equal(box "jacenty", command.Parameters.["name"].Value)
        Assert.Equal(box "jacenty@gmail.com", command.Parameters.["email"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters.["created"].Value)


    [<Fact>]
    let ``Converters``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Simple<DateOnly>("date")()).SetValue(DateOnly.FromDateTime(DateTime.Today), command)

        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Fact>]
    let ``Tuples & Converters``() = 

        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Tuple<int, DateOnly>("id", "date")()).SetValue((1, DateOnly.FromDateTime(DateTime.Today)), command)

        Assert.Equal(box 1, command.Parameters["id"].Value)
        Assert.Equal(box DateTime.Today, command.Parameters["date"].Value)


    [<Fact>]
    let ``Char enums``() =
        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Simple<Status>("status")()).SetValue(Status.Active, command)

        Assert.Equal(box 'A', command.Parameters["status"].Value)

    [<Fact>]
    let ``Int enums``() =
        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Simple<Role>("role")()).SetValue(Role.Admin, command)

        Assert.Equal(box 3, command.Parameters["role"].Value)


    [<Fact>]
    let ``Attribute enums``() =
        let b = ParamBuilder([])
        use command = connection.CreateCommand()

        (b.Simple<Access>("access")()).SetValue(Access.Read, command)

        Assert.Equal(box "RD", command.Parameters["access"].Value)
