namespace DbFun.TestTools.Models

open System
open DbFun.Core.Models

type Status = 
    | New       = 'N'
    | Active    = 'A'
    | Blocked   = 'B'
    | Deleted   = 'D'

type Role = 
    | Guest     = 1
    | Regular   = 2
    | Admin     = 3

type Access =     
    | [<EnumValue("NO")>] NoAccess  = 0
    | [<EnumValue("RD")>] Read      = 1 
    | [<EnumValue("WR")>] Write     = 2
    | [<EnumValue("RW")>] ReadWrite = 3

type User = 
    {
        userId  : int
        name    : string
        email   : string
        created : DateTime
    }

type UserWithRoles = 
    {
        userId  : int
        name    : string
        email   : string
        created : DateTime
        roles   : string list
    }

