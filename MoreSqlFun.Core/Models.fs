namespace Sql2Fun.Core.Models

open System

[<AttributeUsage(AttributeTargets.Field)>]
type EnumValueAttribute(value: string) =
    inherit Attribute()
    member __.Value = value


