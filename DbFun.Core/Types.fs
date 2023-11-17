namespace DbFun.Core

open System

/// <summary>
/// Reflection helper functions.
/// </summary>
module Types = 

    /// <summary>
    /// Checks if a given type is a simple type i.e. whether it can be a column value.
    /// Includes .NET basic types, string, DateTime, TimeSpan, Guid and byte array.
    /// </summary>
    /// <param name="typ">
    /// The type to be checked.
    /// </param>
    let isSimpleType (typ: Type) = typ.IsPrimitive || List.contains typ [ typeof<string>; typeof<DateTime>; typeof<DateTimeOffset>; typeof<TimeSpan>; typeof<Guid>; typeof<byte array> ] 

    /// <summary>
    /// Checks if a given type is an option type.
    /// </summary>
    /// <param name="typ">
    /// The type to be checked.
    /// </param>
    let isOptionType (typ: Type) = typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

    /// <summary>
    /// Checks whether a given type is a collection type, i.e. array, sequence or its descendants, but not strig or byte array 
    /// (they can be stored as simple column values).
    /// </summary>
    /// <param name="typ">
    /// The type to be checked.
    /// </param>
    let isCollectionType (typ: Type) = 
        typ.IsArray ||
        (typ.IsGenericType && 
         typedefof<seq<_>>.MakeGenericType(typ.GetGenericArguments().[0]).IsAssignableFrom(typ) && 
         typ <> typeof<string> && 
         typ <> typeof<byte array>)

    /// <summary>
    /// Returns element type of a collection type
    /// </summary>
    /// <param name="collectionType">
    /// The collection type.
    /// </param>
    let getElementType(collectionType: Type) = 
        if collectionType.IsArray then
            collectionType.GetElementType()
        else
            collectionType.GetGenericArguments().[0]
