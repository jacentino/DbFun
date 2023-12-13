namespace DbFun.MsSql.IntegrationTests

open System
open Xunit
open DbFun.Core
open DbFun.MsSql.IntegrationTests.Models
open Commons

module Tests = 

    [<Fact>]
    let ``Query returning one row`` () =
        let blog = TestQueries.getBlog 1 |> run |> Async.RunSynchronously
        Assert.Equal(1, blog.id)

    [<Fact>]
    let ``Query returning scalar`` () =
        let name = TestQueries.getBlogName 1 |> run |> Async.RunSynchronously
        Assert.Equal("functional-data-access-with-dbfun", name)

    [<Fact>]
    let ``Query returning many rows`` () =
        let blogs = TestQueries.getAllBlogs() |> run |> Async.RunSynchronously
        Assert.Equal(1, blogs |> Seq.length)

    [<Fact>]
    let ``Query returning one row optionally - row exists`` () =
        let blog = TestQueries.getBlogOptional 1 |> run |> Async.RunSynchronously
        Assert.NotNull(blog)

    [<Fact>]
    let ``Query returning one row optionally - row doesn't exist`` () =
        let blog = TestQueries.getBlogOptional 10 |> run |> Async.RunSynchronously
        Assert.Null(blog)

    [<Fact>]
    let ``Query returning many results using join to combine them``() = 
        let pl = TestQueries.getPostsWithTagsAndComments 1 |> run |> Async.RunSynchronously |> Seq.toList
        Assert.Equal(2, pl |> List.length)
        let p = pl |> List.head
        Assert.Equal(1, p.blogId)
        Assert.Equal(3, p.tags |> List.length)


    [<Fact>]
    let ``Query returning many results using applicative functor to combine them``() = 
        let p = TestQueries.getOnePostWithTagsAndComments 1 |> run |> Async.RunSynchronously
        Assert.Equal(1, p.blogId)
        Assert.Equal(3, p.tags |> List.length)


    [<Fact>]
    let ``Template-based query returning one result``() = 
        let criteria = 
            { Criteria.Default with
                author          = Some "jac"
                statuses        = [ PostStatus.Published ]
                tags            = [ "framework" ]
                sortOrder       = { field = SortField.Name; direction = SortDirection.Asc }
            }
        let p = TestQueries.findPosts criteria |> run |> Async.RunSynchronously |> Seq.head
        Assert.Equal(1, p.blogId)


    [<Fact>]
    let ``Query filtering by DateTimeOffset`` () =
        let blogs = 
            TestQueries.getBlogsBefore(DateTimeOffset.Now) 
            |> run 
            |> Async.RunSynchronously
        Assert.Equal(1, blogs |> Seq.length)


    [<Fact>]
    let ``Queries utilizing TVP-s``() = 
        let tags = [
            (2, "Dapper")
            (2, "EntityFramework")
            (2, "FSharp.Data.SqlClient")
        ]
        TestQueries.updateTags 2 tags |> run |> Async.RunSynchronously
        let result = TestQueries.getTags 2 |> run |> Async.RunSynchronously
        Assert.Equal<string list>(tags |> List.map snd, result)


    [<Fact>]
    let ``Stored procedure with transformed result``() = 
        let pl = TestQueries.getAllPosts 1 |> run |> Async.RunSynchronously
        Assert.Equal(2, pl |> List.length)
        let p = pl |> List.head
        Assert.Equal(1, p.comments |> List.length)


    [<Fact>]
    let ``Combining queries together with dbsession``() = 
        let post1, post2 = 
            dbsession {
                let! post1 = TestQueries.getOnePostWithTagsAndComments 1
                let! post2 = TestQueries.getOnePostWithTagsAndComments 2
                return post1, post2
            } |> run |> Async.RunSynchronously
        Assert.Equal("Yet another sql framework", post1.title)
        Assert.Equal("What's wrong with existing frameworks", post2.title)


    [<Fact>]
    let ``Updating in transaction``() = 
        let tags = [
            (2, "Dapper")
            (2, "EntityFramework")
            (2, "FSharp.Data.SqlClient")
        ]
        TestQueries.updateTags 2 tags |> DbCall.InTransaction |> run |> Async.RunSynchronously
        let result = TestQueries.getTags 2 |> run |> Async.RunSynchronously
        Assert.Equal<string list>(tags |> List.map snd, result)


    [<Fact>]
    let ``Using dbsession and transactions together``() = 
        let post1, post2 = 
            dbsession {
                let! post1 = TestQueries.getOnePostWithTagsAndComments 1
                let! post2 = TestQueries.getOnePostWithTagsAndComments 2
                return post1, post2
            } |> DbCall.InTransaction |> run |> Async.RunSynchronously
        Assert.Equal("Yet another sql framework", post1.title)
        Assert.Equal("What's wrong with existing frameworks", post2.title)


    [<Fact>]
    let ``Compile-time errors - logging & derived QueryBuilder``() = 

              
        TestQueries.invalidQuery |> ignore

        let lineNo, fileName, _ = TestQueries.query.CompileTimeErrors |> List.head

        Assert.Equal(TestQueries.invalidLine, lineNo)
        Assert.Contains("TestQueries.fs", fileName)
