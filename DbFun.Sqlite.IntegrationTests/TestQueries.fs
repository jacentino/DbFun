namespace DbFun.Sqlite.IntegrationTests

open Commons
open Models

module TestQueries =    

    let getBlog = query.Sql<int, Blog>("select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from blog where id = @blogid", "blogid")

    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (id, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
        values (@id, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")        

    let insertPost = query.Sql<Post, unit>(
        "insert into post (id, blogId, name, title, content, author, createdAt, modifiedAt, modifiedBy, status)
         values (@id, @blogId, @name, @title, @content, @author, @createdAt, @modifiedAt, @modifiedBy, @status)")
        

