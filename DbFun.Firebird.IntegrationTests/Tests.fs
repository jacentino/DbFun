namespace DbFun.Firebird.IntegrationTests

open System
open Xunit
open Commons

module Tests = 

    let runSync f = run f |> Async.RunSynchronously

    [<Fact>]
    let ``Simple queries to Firebird return valid results``() = 
        let blog = TestQueries.getBlog 1 |> runSync
        Assert.Equal("functional-data-access-with-sqlfun", blog.name)        

    [<Fact>]
    let ``Inserts to Firebird work as expected``() =    

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
        } |> runSync


    [<Fact>]
    let ``Batch inserts to Firebird work as expected``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        let results = 
            TestQueries.batchInsertBlogs [
                {
                    id = 4
                    name = "test-blog-4"
                    title = "Testing batch insert 4"
                    description = "Added to check if inserts work properly."
                    owner = "jacentino"
                    createdAt = DateTime.Now
                    modifiedAt = None
                    modifiedBy = None
                }
                {
                    id = 5
                    name = "test-blog-5"
                    title = "Testing batch insert 5"
                    description = "Added to check if inserts work properly."
                    owner = "placentino"
                    createdAt = DateTime.Now
                    modifiedAt = None
                    modifiedBy = None
                }
            ] |> runSync

        Assert.True(results.AllSuccess)

