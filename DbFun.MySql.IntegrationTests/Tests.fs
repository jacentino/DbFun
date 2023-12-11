namespace DbFun.MySql.IntegrationTests


open System
open Xunit
open Commons

module Tests = 

    let runSync f = run f |> Async.RunSynchronously

    [<Fact>]
    let ``Simple queries to MySql return valid results``() =
        let b = TestQueries.getBlog 1 |> runSync
        Assert.Equal(1, b.id)

    [<Fact>]
    let ``Stored procedure calls to MySql return valid results``() =
        let b = TestQueries.spGetBlog 1 |> runSync
        Assert.Equal(1, b.id)

    [<Fact>]
    let ``Stored procedure calls to MySql work as expected``() =

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.spInsertBlog {
            id = 4
            name = "test-blog-4"
            title = "Testing simple insert 4"
            description = "Added to check if inserts work properly."
            owner = "jacentino"
            createdAt = DateTime.Now
            modifiedAt = None
            modifiedBy = None
            posts = []
        }  |> runSync

    
    [<Fact>]
    let ``Inserts to MySql work as expected``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.insertBlog {
            id = 4
            name = "test-blog-4"
            title = "Testing simple insert 4"
            description = "Added to check if inserts work properly."
            owner = "jacentino"
            createdAt = DateTime.Now
            modifiedAt = None
            modifiedBy = None
            posts = []
        } |> runSync
    