namespace DbFun.Npgsql.IntegrationTests

open System
open Commons

module Models = 

    type PostStatus = 
        | New       = 'N'
        | Published = 'P'
        | Archived  = 'A'

    type Post = {
        postId: int
        blogId: int
        name: string
        title: string
        content: string
        author: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
        status: PostStatus
    }

    type Blog = {
        blogId: int
        name: string
        title: string
        description: string
        owner: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
        posts: Post list
    }


module Tooling = 
    
    let getNumberOfBlogs = query.Sql<unit, int> "select count(*) from blog"

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit> "delete from blog where blogid > 1"

    let deleteAllUsers = 
        query.Sql<unit, unit> "delete from userprofile"
