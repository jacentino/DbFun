namespace DbFun.Core.Builders

open System

/// <summary>
/// Record naming conventions.
/// </summary>
type RecordNaming =     
    | Fields    
    | Prefix    
    | Path      

/// <summary>
/// Discriminated union naming conventions.
/// </summary>
[<Flags>]
type UnionNaming = 
    | Fields    = 0
    | Prefix    = 1
    | Path      = 2
    | CaseNames = 4
    