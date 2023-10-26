namespace DbFun.Core.Models

open System

[<AttributeUsage(AttributeTargets.Field)>]
type EnumValueAttribute(value: string) =
    inherit Attribute()
    member __.Value = value


