namespace MoreSqlFun.Core.Tests.Models

open System
open MoreSqlFun.Core.Models

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
    | [<EnumValue("RW")>] ReadWrite = 2

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

