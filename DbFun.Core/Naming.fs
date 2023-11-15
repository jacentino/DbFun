namespace DbFun.Core.Builders

open System

type RecordNaming = 
    | Fields    
    | Prefix    
    | Path      

[<Flags>]
type UnionNaming = 
    | Fields    = 0
    | Prefix    = 1
    | Path      = 2
    | CaseNames = 4
    