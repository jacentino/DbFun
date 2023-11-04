namespace DbFun.Core.Models

open System

/// <summary>
/// An attribute allowing to specify string values, representing database values of enum literals.
/// </summary>
[<AttributeUsage(AttributeTargets.Field)>]
type EnumValueAttribute(value: string) =
    inherit Attribute()
    member __.Value = value


