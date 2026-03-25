namespace DbFun.MsSql.IntegrationTests

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open FSharp.Control
open DbFun.Core
open DbFun.TestTools
open DbFun.MsSql.IntegrationTests.Models
open DbFun.Core.Builders
open DbFun.MsSql.Builders
open DbFun.Core.Builders.MultipleResults
open Commons

type Diag() = 
    static member GetLine([<CallerLineNumber; Optional; DefaultParameterValue(0)>] line: int) = line

module TestQueries = 
    
    let config = config.AddParamPropertyMapper<Criteria>()

    let query = QueryBuilder(config).LogCompileTimeErrors()

    let p = any<Post>

    let getBlog = query.Sql<int, Blog>("select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog where id = @id", "id")
            
    let getBlogName = query.Sql<int, string>("select name from Blog where id = @id", "id")

    let getAllBlogs = query.Sql<unit, Blog seq>("select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog") 
            
    let getAllBlogsAsync = query.Sql<unit, Blog AsyncSeq>("select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from Blog") 

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
        Results.PKeyed<int, _> "id"
        |> Results.Join (fun (p, cs) -> { p with comments = buildTree cs }) (Results.FKeyed "postId")
        |> Results.Join p.tags (Results.FKeyed("postId", "name"))
        |> Results.Unkeyed)
            
    let getPostsWithComments = query.Sql<int, Post seq>(
        "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status from post where blogId = @blogId;
         select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt from comment c join post p on c.postId = p.id where p.blogId = @blogId",
        "blogId", 
        Results.PKeyed<int, Post> "id"
        |> Results.Join p.comments (Results.FKeyed<int, Comment> "postId")
        |> Results.Unkeyed)
            

    let unsafeGetPostsWithTagsAndComments = query.DisablePrototypeCalls().Sql<int, Post seq>(
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
            
    let unsafeGetOnePostWithTagsAndComments = query.DisablePrototypeCalls().Sql<int, Post>(
        "select id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status from post where id = @postId;
         select c.id, c.postId, c.parentId, c.content, c.author, c.createdAt from comment c where c.postId = @postId
         select t.postId, t.name from tag t where t.postId = @postId",
        "postId",
        Results.Combine(fun post comments tags -> { post with comments = buildTree comments; tags = tags })
        <*> Results.Single<Post>()
        <*> Results.List<Comment>()
        <*> Results.List<string> "name")

    let findPosts = query.HandleCollectionParams().Sql<Criteria, Post seq>( 
        Templating.define 
            "select p.id, p.blogId, p.name, p.title, p.content, p.author, p.createdAt, p.modifiedAt, p.modifiedBy, p.status from post p
             {{JOIN-CLAUSES}} {{WHERE-CLAUSE}} {{ORDER-BY-CLAUSE}}"
            (Templating.applyWhen (fun (c: Criteria) -> c.Name.IsSome) 
                (Templating.where "p.name like '%' + @Name + '%'")
            >> Templating.applyWhen _.Title.IsSome
                (Templating.where "p.title like '%' + @Title + '%'")
            >> Templating.applyWhen _.Content.IsSome
                (Templating.where "p.content like '%' + @Content + '%'")
            >> Templating.applyWhen _.Author.IsSome 
                (Templating.where "p.author like '%' + @Author + '%'")                
            >> Templating.applyWhen _.CreatedFrom.IsSome
                (Templating.where "p.createdAt >= @CreatedFrom")
            >> Templating.applyWhen _.CreatedTo.IsSome
                (Templating.where "p.createdAt <= @CreatedTo")
            >> Templating.applyWhen _.ModifiedFrom.IsSome
                (Templating.where "p.modifiedAt >= @ModifiedFrom")
            >> Templating.applyWhen _.ModifiedTo.IsSome 
                (Templating.where "p.modifiedAt <= @ModifiedTo")
            >> Templating.applyWhen (_.Statuses.IsEmpty >> not) 
                (Templating.where "p.status in (@Statuses)")
            >> Templating.applyWhen (_.Tags.IsEmpty >> not) 
                (Templating.join "join Tag t on t.postId = p.id" >> Templating.where "t.name in (@Tags)")
            >> Templating.applyWith _.SortOrder (Templating.orderBy { field = SortField.CreatedAt; direction = SortDirection.Asc }))) 


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

    let invalidLine = Diag.GetLine() + 1
    let invalidQuery = query.Timeout(30).Sql<unit, Blog list>("select * from NotExistingTable")