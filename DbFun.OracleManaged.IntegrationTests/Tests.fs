namespace DbFun.OracleManaged.IntegrationTests

open System
open Xunit
open Commons

module Tests = 

    let runSync f = run f |> Async.RunSynchronously

    [<Fact>]
    let ``Simple queries to Oracle return valid results``() = 
        let blog = TestQueries.getBlog 1 |> runSync
        Assert.Equal("functional-data-access-with-sqlfun", blog.name)        

    [<Fact>]
    let ``Inserts to Oracle work as expected``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.insertBlog {
            blogId = 4
            name = "test-blog-4"
            title = "Testing simple insert 4"
            description = "Added to check if inserts work properly."
            owner = "jacentino"
            createdAt = DateTime.Now
            modifiedAt = None
            modifiedBy = None
        } |> runSync

    [<Fact>]
    let ``Array parameters allow to add multiple records``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.insertBlogsWithArrays  [
            { blogId = 2; name = "test-blog-2"; title = "Testing array parameters 1"; description = "Add to check if VARRAY parameters work as expected (1)."; owner = "jacentino"; createdAt = DateTime.Now; modifiedAt = None; modifiedBy = None }
            { blogId = 3; name = "test-blog-3"; title = "Testing array parameters 2"; description = "Added to check if VARRAY parameters work as expected (2)."; owner = "placentino"; createdAt = DateTime.Now; modifiedAt = None; modifiedBy = None }
        ]
        |> runSync

    [<Fact>]
    let ``BulkCopy allow to add multiple records``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.insertBlogsWithBulkCopy  [
            { blogId = 2; name = "test-blog-2"; title = "Testing array parameters 1"; description = "Add to check if VARRAY parameters work as expected (1)."; owner = "jacentino"; createdAt = DateTime.Now; modifiedAt = None; modifiedBy = None }
            { blogId = 3; name = "test-blog-3"; title = "Testing array parameters 2"; description = "Added to check if VARRAY parameters work as expected (2)."; owner = "placentino"; createdAt = DateTime.Now; modifiedAt = None; modifiedBy = None }
        ]
        |> runSync

    [<Fact>]
    let ``Insert to Oracle with stored procedures works as expected``() =    

        Tooling.deleteAllButFirstBlog() |> runSync

        TestQueries.insertBlogProc  
            5
            ( "test-blog-5"
            , "Testing simple insert 5"
            , "Added to check if inserts work properly."
            , "jacentino"
            ,  DateTime.Now )
        |> runSync