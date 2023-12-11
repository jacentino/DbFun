namespace DbFun.MySql.IntegrationTests

open System
open Commons

module Models = 

    type Comment = {
        id: int
        postId: int
        parentId: int option
        content: string
        author: string
        createdAt: DateTime
        replies: Comment list
    }

    type Tag = {
        postId: int
        name: string
    }

    type PostStatus = 
        | New       = 'N'
        | Published = 'P'
        | Archived  = 'A'

    type Post = {
        id: int
        blogId: int
        name: string
        title: string
        content: string
        author: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
        status: PostStatus
        comments: Comment list
        tags: Tag list
    }

    type Blog = {
        id: int
        name: string
        title: string
        description: string
        owner: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
        posts: Post list
    }


open Models

module Tooling = 

    let getNumberOfBlogs = query.Sql<unit, int>("select count(*) from blog")
    
    let deleteAllButFirstBlog = query.Sql<unit, unit>("delete from blog where id > 1")

