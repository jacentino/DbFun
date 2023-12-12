namespace DbFun.MySqlConnector.IntegrationTests


open System
open Xunit
open Commons
open System.Diagnostics
open System.IO
open Models

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
    
    [<Fact>]
    let ``BulkCopy inserts records without subrecords``() = 

        Tooling.deleteAllButFirstBlog() |> runSync

        let blogsToAdd = 
            [  for i in 2..200 do
                {
                    id = i
                    name = sprintf "blog-%d" i
                    title = sprintf "Blog no %d" i
                    description = sprintf "Just another blog, added for test - %d" i
                    owner = "jacenty"
                    createdAt = System.DateTime.Now
                    modifiedAt = None
                    modifiedBy = None
                    posts = []          
                }
            ]

        let sw = Stopwatch()
        sw.Start()
        TestQueries.bulkInsertBlogs blogsToAdd |> runSync |> ignore
        sw.Stop()
        printfn "Elapsed time %O" sw.Elapsed
        
        let numOfBlogs = Tooling.getNumberOfBlogs() |> runSync
        Tooling.deleteAllButFirstBlog() |> runSync
        Assert.Equal(200, numOfBlogs)

    [<Fact>]
    let ``BulkCopy handles byte array fields properly``() = 

        Tooling.deleteAllUsers() |> runSync

        let assemblyFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
        let users = [
                "jacirru",
                "Jacirru Placirru",
                "jacirru.placirru@pp.com",
                File.ReadAllBytes(Path.Combine(assemblyFolder, "jacenty.jpg"))
        ]
        TestQueries.bulkInsertUsers users |> runSync 

