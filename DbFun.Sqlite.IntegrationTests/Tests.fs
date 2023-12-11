namespace DbFun.Sqlite.IntegrationTests

open System
open Xunit
open Commons
open Models

module Tests = 

    let runSync f = run f |> Async.RunSynchronously

    [<Fact>]
    let ``Simple queries to Sqlite return valid results``() = 
        let blog = TestQueries.getBlog 1 |> runSync
        Assert.Equal("functional-data-access-with-sqlfun", blog.name)        
    
    [<Fact>]
    let ``Inserts to sqlite work as expected``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.insertBlog {
            id = 4
            name = "test-blog-4"
            title = "Testing simple insert 4"
            description = "Added to check if inserts work properly."
            owner = "jacentino"
            createdAt = System.DateTime.Now
            modifiedAt = None
            modifiedBy = None
        } |> run

    [<Fact>]
    let ``Inserts to tables with foregin keys work as expected``() = 
        
        Tooling.deleteAllPosts() |> runSync

        TestQueries.insertPost {
            id = 1
            blogId = 1
            name = "test-post-1"
            title = "Checking inserts to tables with foreign key constraints"
            content = "Just checking"
            author = "jacentino"
            createdAt = System.DateTime.Now
            modifiedAt = None
            modifiedBy = None
            status = PostStatus.New
        } |> runSync


