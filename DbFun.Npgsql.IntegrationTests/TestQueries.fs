namespace DbFun.Npgsql.IntegrationTests

open System
open DbFun.Core
open DbFun.Npgsql.Builders
open Commons
open Models
open DbFun.TestTools.Models
open DbFun.Core.Builders

module TestQueries = 

    let query = query.LogCompileTimeErrors()

    let getBlog = query.Sql<int, Blog>("select blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy from blog where blogid = @id", "id")

    let fnGetBlog = query.Sql<int, Blog>("select * from getblog(@id)", "id")
        
    let getPosts= query.Sql<int list, Post list>(
        "select p.postid, p.blogId, p.name, p.title, p.content, p.author, p.createdAt, p.modifiedAt, p.modifiedBy, p.status
            from post p join unnest(@ids) ids on p.postid = ids",
        "ids")

    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@blogId, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let insertBlogAutoInc = query.Sql<Blog, unit>(
        "insert into blog (blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (2, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy);
         select 2")

    let insertBlogs = query.Sql<Blog list, unit>(
        "insert into blog (blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         select * from  unnest(@blogId, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let bulkInsertBlogs = bulkImport.WriteToServer<Blog>()

    let bulkInsertUsers = bulkImport.WriteToServer(BulkImportParams.Tuple<string, string, string, byte array>("id", "name", "email", "avatar"), "userprofile")

    let getIntArray = query.Sql<unit, int array list>("select array[1, 2, 3]")

    let getCharArray = query.Sql<unit, char array list>("select array['A', 'B', 'C']")

    let getStringArray = query.Sql<unit, string array list>("select array['A', 'B', 'C']")

    let getDecimalArray = query.Sql<unit, decimal array list>("select array[1, 2, 3]")

    let getIntList = query.Sql<unit, int list list>("select array[1, 2, 3]")

    let getIntSeq = query.Sql<unit, int seq list>("select array[1, 2, 3]")

    let getCharEnumList = query.Sql<unit, PostStatus list list>("select array['N', 'P', 'A']")

    let getUnionEnumList = query.Sql<unit, Access list list>("select array['RD', 'WR']")

    let getDateOnlySeq = query.Sql<unit, DateOnly seq list>("select array[TIMESTAMP '2004-10-19 00:00:00+02']")

    let getIntArrayExplicit = query.Sql("select array[1, 2, 3]", Params.Unit, Results.Single(Rows.PgArray<int>("")))

    let getIntListExplicit = query.Sql("select array[1, 2, 3]", Params.Unit, Results.Single(Rows.PgList<int>("")))

    let getIntArrayHalfExplicit = query.Sql("select array[1, 2, 3]", Params.Unit, Results.Single<int array>(""))

    let getIntListHalfExplicit = query.Sql("select array[1, 2, 3]", Params.Unit, Results.Single<int list>(""))
