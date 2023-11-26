namespace DbFun.Core.IntegrationTests

open System
open DbFun.Core.IntegrationTests.Models
open Commons

module Tests = 

    open Xunit

    [<Fact>]
    let ``Warm up`` () =
        ignore TestQueries.getBlog

    [<Fact>]
    let ``Query returning one row`` () =
        let blog = TestQueries.getBlog 1 |> run |> Async.RunSynchronously
        Assert.Equal(1, blog.id)

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
