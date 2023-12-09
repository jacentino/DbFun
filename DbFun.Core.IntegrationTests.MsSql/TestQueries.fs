namespace DbFun.Core.IntegrationTests.MsSql

open System
open DbFun.Core
open DbFun.TestTools
open DbFun.Core.IntegrationTests.MsSql.Models
open DbFun.Core.Builders
open DbFun.MsSql.Builders

open DbFun.Core.Builders.MultipleResults

open Commons

module TestQueries = 
    
    let query = query.LogCompileTimeErrors()

    let p = any<Post>

    let getBlog = query.Sql<int, Blog>("select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog where id = @id", "id")
            
    let getBlogName = query.Sql<int, string>("select name from Blog where id = @id", "id")

    let getAllBlogs = query.Sql<unit, Blog seq>("select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog") 
            

    let getBlogsBefore = query.Sql(
        "select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog where createdAt <= @createdTo", 
        Params.Auto<DateTimeOffset> "createdTo", 
        Results.List<BlogTZ>()) 
            

    let getBlogOptional = query.Sql<int, Blog option>("select * from Blog where id = @id", "id") 


    let rec buildSubtree (parenting: Map<int option, Comment list>) (cmt: Comment) = 
        { cmt with replies = parenting |> Map.tryFind (Some cmt.id) |> Option.map (List.map (buildSubtree parenting)) |> Option.defaultValue [] }

    let buildTree (comments: Comment list) = 
        let (roots, children) = comments |> List.groupBy (fun c -> c.parentId) |> List.partition (fst >> Option.isNone)
        let parenting = children |> Map.ofList
        roots |> List.map snd |> List.collect id |> List.map (buildSubtree parenting)
            

    let getPostsWithTagsAndComments = query.Sql<int, Post seq>(
        "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status from post where blogId = @blogId;
         select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt from comment c join post p on c.postId = p.id where p.blogId = @blogId
         select t.postId, t.name from tag t join post p on t.postId = p.id where p.blogId = @blogId",
        "blogId", 
        Results.PKeyed<int, Post> "id"
        |> Results.Join (fun (p, cs) -> { p with comments = buildTree cs }) (Results.FKeyed "postId")
        |> Results.Join p.tags (Results.FKeyed("postId", "name"))
        |> Results.Unkeyed)
            

    let getOnePostWithTagsAndComments = query.Sql<int, Post>(
        "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status from post where id = @postId;
         select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt from comment c where c.postId = @postId
         select t.postId, t.name from tag t where t.postId = @postId",
        "postId",
        Results.Combine(fun post comments tags -> { post with comments = buildTree comments; tags = tags })
        <*> Results.Single<Post>()
        <*> Results.List<Comment>()
        <*> Results.List<string> "name")
            

    let findPosts = query.TemplatedSql ( 
        Templating.define 
            "select p.id, p.blogId, p.name, p.title, p.content, p.author, p.createdAt, p.modifiedAt, p.modifiedBy, p.status from post p
             {{JOIN-CLAUSES}} {{WHERE-CLAUSE}} {{ORDER-BY-CLAUSE}}"
            (Templating.applyWhen (fun c -> c.name.IsSome) 
                (Templating.where "p.name like '%' + @name + '%'")
            >> Templating.applyWhen (fun c -> c.title.IsSome) 
                (Templating.where "p.title like '%' + @title + '%'")
            >> Templating.applyWhen (fun c -> c.content.IsSome) 
                (Templating.where "p.content like '%' + @content + '%'")
            >> Templating.applyWhen (fun c -> c.author.IsSome) 
                (Templating.where "p.author like '%' + @author + '%'")                
            >> Templating.applyWhen (fun c -> c.createdFrom.IsSome) 
                (Templating.where "p.createdAt >= @createdFrom")
            >> Templating.applyWhen (fun c -> c.createdTo.IsSome) 
                (Templating.where "p.createdAt <= @createdTo")
            >> Templating.applyWhen (fun c -> c.modifiedFrom.IsSome) 
                (Templating.where "p.modifiedAt >= @modifiedFrom")
            >> Templating.applyWhen (fun c -> c.modifiedTo.IsSome) 
                (Templating.where "p.modifiedAt <= @modifiedTo")
            >> Templating.applyWhen (fun c -> not c.statuses.IsEmpty) 
                (Templating.where "p.status in (@statuses)")
            >> Templating.applyWhen (fun c -> not c.tags.IsEmpty) 
                (Templating.join "join Tag t on t.postId = p.id" >> Templating.where "t.name in (@tags)")
            >> Templating.applyWith (fun c -> c.sortOrder.ToString()) "p.createdAt asc" Templating.orderBy),
        Params.Record<Criteria>(), 
        Results.Seq<Post>()) 


    let getAllPosts = 
        query.Proc("GetAllPosts", 
            Params.Int("blogid"),
            OutParams.Unit,
            Results.PKeyed<int, Post>("id")
            |> Results.Join (fun (p, cs) -> { p with comments = buildTree cs }) (Results.FKeyed<int, Comment>("postId"))
            |> Results.Join p.tags (Results.FKeyed<int, string>("postId", "name"))
            |> Results.Unkeyed) 
        >> DbCall.Map (fst >> Seq.toList)

    let getTags = query.Sql<int, string list>("select name from Tag where postId = @postId", "postId", "name")

    let updateTags = query.Sql(    
        "delete from tag where postId = @id;
        insert into tag (postId, name) select @id, name from @tags",
        Params.Int("id"), Params.TableValuedList(TVParams.Tuple<int, string>("postId", "name"), "tags", "Tag"),
        Results.Unit)
