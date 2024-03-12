namespace DbFun.CrossDatabase.IntegrationTests

open System
open Xunit
open Commons 
open Models
open DbFun.Core

module Tests = 

    let runSync f = run f |> Async.RunSynchronously
    
    [<Fact>]
    let ``Inserts to different databases work as expected``() =    

        MsSqlQueries.deleteAllButFirstBlog() |> runSync
        MySqlQueries.deleteAllButFirstBlog() |> runSync
        PostgresQueries.deleteAllButFirstBlog() |> runSync

        let blog = {
            id = 4
            name = "test-blog-4"
            title = "Testing simple insert 4"
            description = "Added to check if inserts work properly."
            owner = "jacentino"
            createdAt = DateTime.Now
            modifiedAt = None
            modifiedBy = None
        }

        dbsession {
            do! MsSqlQueries.insertBlog blog 
            do! MySqlQueries.insertBlog blog 
            do! PostgresQueries.insertBlog blog 
        } |> runSync

        let msSqlNumOfBlogs = MsSqlQueries.getNumberOfBlogs() |> runSync
        let mySqlNumOfBlogs = MySqlQueries.getNumberOfBlogs() |> runSync
        let pgSqlNumOfBlogs = PostgresQueries.getNumberOfBlogs() |> runSync

        Assert.Equal(2, msSqlNumOfBlogs)
        Assert.Equal(2, mySqlNumOfBlogs)
        Assert.Equal(2, pgSqlNumOfBlogs)


    [<Fact>]
    let ``Parallel inserts to the same database work as expected``() =    

        MsSqlQueries.deleteAllButFirstBlog() |> runSync

        let inserts = [
            for i in 4..6 do
                MsSqlQueries.insertBlog {
                    id = i
                    name = $"test-blog-{i}"
                    title = $"Testing simple insert {i}"
                    description = "Added to check if inserts work properly."
                    owner = "jacentino"
                    createdAt = DateTime.Now
                    modifiedAt = None
                    modifiedBy = None
                }
        ]

        let result = inserts |> DbCall.Parallel |> runSync
        
        let msSqlNumOfBlogs = MsSqlQueries.getNumberOfBlogs() |> runSync

        Assert.Equal(3, result.Length)
        Assert.Equal(4, msSqlNumOfBlogs)

