﻿namespace DbFun.TestTools.Models

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
    | [<UnionCaseTag("NO")>] NoAccess 
    | [<UnionCaseTag("RD")>] Read     
    | [<UnionCaseTag("WR")>] Write    
    | [<UnionCaseTag("RW")>] ReadWrite

type PaymentType = 
    | [<UnionCaseTag("NP")>] NoPayment
    | [<UnionCaseTag("CS")>] Cash of string
    | [<UnionCaseTag("CC")>] CreditCard of number: string * cvc: string

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

type UserId = UserId of int

type Signature = 
    {
        createdAt : DateTime
        createdBy : string
        updatedAt : DateTime
        updatedBy : string
    }

type Account = 
    {
        userId      : string
        password    : string
        signature   : Signature
    }