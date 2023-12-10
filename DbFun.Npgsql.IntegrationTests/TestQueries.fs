namespace DbFun.Npgsql.IntegrationTests

open DbFun.Core
open Commons
open Models

module TestQueries = 

    let query = query.LogCompileTimeErrors()

    let getBlog = query.Sql<int, Blog>("select blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy from blog where blogid = @id", "id")

    let fnGetBlog = query.Sql<int, Blog>("select * from getblog(@id)", "id")
        
    let getPosts= query.Sql<int array, Post list>(
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

    let bulkInsertBlogs = bulkImport.WriteToServer<Blog>()

    let bulkInsertUsers = bulkImport.WriteToServer<UserProfile>()