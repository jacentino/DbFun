namespace Sql2Fun.Core.IntegrationTests

open Sql2Fun.Core.IntegrationTests.Models
open Commons

module TestQueries = 
    
    open Sql2Fun.Core.Builders
    open Sql2Fun.Core.Builders.MultipleResults

    let query = query.LogCompileTimeErrors()

    let p = any<Post>

    let getBlog = 
        query.Sql(Params.Int "id") (Results.One<Blog> "")
            "select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog where id = @id"

    let getAllBlogs = 
        query.Sql Params.Unit (Results.Many<Blog> "") 
            "select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog"

    let getBlogOptional = 
        query.Sql(Params.Int "id") (Results.TryOne<Blog> "") 
            "select * from Blog where id = @id"

    let getPostsWithTagsAndComments = 
        query.Sql (Params.Int "blogId") 
                  (Results.PKeyed<int, Post>("id", "")
                    |> Results.Join p.comments (Results.FKeyed("postId", ""))
                    |> Results.Join p.tags (Results.FKeyed("postId", "name"))
                    |> Results.Unkeyed)
            "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status from post where blogId = @blogId;
             select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt from comment c join post p on c.postId = p.id where p.blogId = @blogId
             select t.postId, t.name from tag t join post p on t.postId = p.id where p.blogId = @blogId"

    let getOnePostWithTagsAndComments = 
        query.Sql (Params.Int "postId") 
                  (Results.Combine(fun post comments tags -> { post with comments = comments |> Seq.toList; tags = tags |> Seq.toList })
                    <*> Results.One<Post>("")
                    <*> Results.Many<Comment>("")
                    <*> Results.Many<string>("name"))                    
            "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status from post where id = @postId;
             select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt from comment c where c.postId = @postId
             select t.postId, t.name from tag t where t.postId = @postId"

module Tests = 

    open Xunit

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

