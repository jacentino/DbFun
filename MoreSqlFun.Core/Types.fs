namespace MoreSqlFun.Core

open System

module Types = 

    let isSimpleType (t: Type) = t.IsPrimitive || List.contains t [ typeof<string>; typeof<DateTime>; typeof<TimeSpan>; typeof<Guid>; typeof<byte array> ] 

    let isOptionType (t: Type) = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Option<_>>

    let isCollectionType (t: Type) = 
        t.IsArray ||
        (t.IsGenericType && 
         typedefof<seq<_>>.MakeGenericType(t.GetGenericArguments().[0]).IsAssignableFrom(t) && 
         t <> typeof<string> && 
         t <> typeof<byte array>)

    let getElementType(collectionType: Type) = 
        if collectionType.IsArray then
            collectionType.GetElementType()
        else
            collectionType.GetGenericArguments().[0]
        