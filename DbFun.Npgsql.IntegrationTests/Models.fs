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

    type UserProfile(          
            id  : string,
            name    : string,
            email   : string,
            avatar  : byte array) = 
        member __.Id        = id
        member __.Name      = name
        member __.Email     = email
        member __.Avatar    = avatar
        static member Create(userId, name, email, created) = UserProfile(userId, name, email, created)
        override __.Equals(other: obj) = 
            match other with
            | :? UserProfile as u -> id = u.Id && name = u.Name && email = u.Email && avatar = u.Avatar
            | _ -> false
        override __.GetHashCode() =
            id.GetHashCode() ^^^ name.GetHashCode() ^^^ email.GetHashCode() ^^^ avatar.GetHashCode() 

module Tooling = 
    
    let getNumberOfBlogs = query.Sql<unit, int> "select count(*) from blog"

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit> "delete from blog where blogid > 1"

    let deleteAllUsers = 
        query.Sql<unit, unit> "delete from userprofile"
