namespace DbFun.Npgsql.IntegrationTests

open System
open System.IO
open System.Diagnostics
open Xunit
open Commons
open Models
open DbFun.TestTools.Models

module Tests = 

    let runSync f = run f |> Async.RunSynchronously

    [<Fact>]
    let ``TestQueries passes compile-time checks``() =
        Assert.True(TestQueries.query.CompileTimeErrors.IsEmpty, sprintf "%A" TestQueries.query.CompileTimeErrors)

    [<Fact>]
    let ``Simple queries to PostgreSQL return valid results``() =
        let b = TestQueries.getBlog 1 |> runSync
        Assert.Equal(1, b.blogId)

    [<Fact>]
    let ``Queries using PostgreSQL arrays return valid results``() =
        let posts = TestQueries.getPosts [ 1; 2 ] |> runSync
        Assert.Equal<int list>([ 1; 2 ], posts |> List.map (fun p -> p.postId))

    [<Fact>]
    let ``Function calls to PostgreSQL return valid results``() =
        let b = TestQueries.fnGetBlog 1 |> runSync
        Assert.Equal(1, b.blogId)
    
    [<Fact>]
    let ``Inserts to PostgrSQL work as expected``() =    

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
            posts = []
        } |> runSync

    [<Fact>]
    let ``BulkImport inserts records without subrecords``() = 

        Tooling.deleteAllButFirstBlog() |> runSync

        let blogsToAdd = 
            [  for i in 2..200 do
                {
                    blogId = i
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
        TestQueries.bulkInsertBlogs blogsToAdd |> runSync
        sw.Stop()
        printfn "Elapsed time %O" sw.Elapsed
        
        let numOfBlogs = Tooling.getNumberOfBlogs() |> runSync
        Tooling.deleteAllButFirstBlog() |> runSync
        Assert.Equal(200, numOfBlogs)


    [<Fact>]
    let ``PostgreSQL array can be used to insert records``() = 

        Tooling.deleteAllButFirstBlog() |> runSync

        let blogsToAdd = 
            [  for i in 2..200 do
                {
                    blogId = i
                    name = sprintf "blog-%d" i
                    title = sprintf "Blog no %d" i
                    description = sprintf "Just another blog, added for test - %d" i
                    owner = "jacenty"
                    createdAt = System.DateTime.Now
                    modifiedAt = Some System.DateTime.Now
                    modifiedBy = Some "jacenty"
                    posts = []          
                }
            ]

        let sw = Stopwatch()
        sw.Start()
        TestQueries.insertBlogs blogsToAdd |> runSync
        sw.Stop()
        printfn "Elapsed time %O" sw.Elapsed
        
        let numOfBlogs = Tooling.getNumberOfBlogs() |> runSync
        Tooling.deleteAllButFirstBlog() |> runSync
        Assert.Equal(200, numOfBlogs)


    [<Fact>]
    let ``BulkImport handles byte array fields properly``() = 

        Tooling.deleteAllUsers() |> runSync

        let assemblyFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
        let users = [
                "jacirru",
                "Jacirru Placirru",
                "jacirru.placirru@pp.com",
                File.ReadAllBytes(Path.Combine(assemblyFolder, "jacenty.jpg"))
        ]
        TestQueries.bulkInsertUsers users |> runSync 

    [<Fact>]
    let ``Int array``() = 
        let value = TestQueries.getIntArray() |> runSync
        Assert.Equal<int array>([[| 1; 2; 3 |]], value)

    [<Fact>]
    let ``Char array``() = 
        let value = TestQueries.getCharArray() |> runSync
        Assert.Equal<char array>([[| 'A'; 'B'; 'C' |]], value)

    [<Fact>]
    let ``String array``() = 
        let value = TestQueries.getStringArray() |> runSync
        Assert.Equal<string array>([[| "A"; "B"; "C" |]], value)

    [<Fact>]
    let ``Decimal array``() = 
        let value = TestQueries.getDecimalArray() |> runSync
        Assert.Equal<decimal array>([[| 1m; 2m; 3m |]], value)

    [<Fact>]
    let ``Int list``() = 
        let value = TestQueries.getIntList() |> runSync
        Assert.Equal<int list>([[ 1; 2; 3 ]], value)

    [<Fact>]
    let ``Int seq``() = 
        let value = TestQueries.getIntSeq() |> runSync
        Assert.Equal<int seq>([seq{ 1; 2; 3 }], value)

    [<Fact>]
    let ``Char enum list``() = 
        let value = TestQueries.getCharEnumList() |> runSync
        Assert.Equal<PostStatus list>([[ PostStatus.New; PostStatus.Published; PostStatus.Archived ]], value)

    [<Fact>]
    let ``Union enum list``() = 
        let value = TestQueries.getUnionEnumList() |> runSync
        Assert.Equal<Access list>([[ Access.Read; Access.Write ]], value)

    [<Fact>]
    let ``Array column type``() = 
        use connection = createConnection()
        connection.Open()
        use command = connection.CreateCommand()
        command.CommandText <- "select array[@item]"
        let param = command.CreateParameter()
        param.ParameterName <- "item"
        param.Value <- TimeSpan.FromHours(1.0)
        command.Parameters.Add(param) |> ignore
        use reader = command.ExecuteReader()
        let fieldType = reader.GetFieldType(0)
        let schema = reader.GetSchemaTable()
        let typeName = schema.Rows[0][24]
        reader.Read() |> ignore
        let value = reader.GetValue(0)
        let array = value :?> Array
        let display = sprintf "%A: %A" value fieldType
        Console.WriteLine(display)
