namespace DbFun.Firebird.IntegrationTests

open System
open Commons

module Models = 

    type Blog = {
        id: int
        name: string
        title: string
        description: string
        owner: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
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
        static member Create(id, name, email, created) = UserProfile(id, name, email, created)
        override __.Equals(other: obj) = 
            match other with
            | :? UserProfile as u -> id = u.Id && name = u.Name && email = u.Email && avatar = u.Avatar
            | _ -> false
        override __.GetHashCode() =
            id.GetHashCode() ^^^ name.GetHashCode() ^^^ email.GetHashCode() ^^^ avatar.GetHashCode() 

module Tooling = 

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit>("delete from blog where id > 1")

    let deleteAllUsers =  query.Sql<unit, unit> "delete from userprofile"



