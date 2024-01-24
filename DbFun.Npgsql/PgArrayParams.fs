namespace DbFun.Npgsql.Builders

open System
open System.Collections.Generic
open NpgsqlTypes
open DbFun.Core
open DbFun.Core.Builders

type MultipleArrays = 
    {
        ArraySize   : int
        Data        : IDictionary<string, NpgsqlDbType * obj>
    }

type IArrayParamSetter<'Arg> = GenericSetters.ISetter<MultipleArrays, 'Arg>

type IArrayParamSetterProvider = GenericSetters.ISetterProvider<unit, MultipleArrays>

type ArrayParamSpecifier<'Arg> = IArrayParamSetterProvider * unit -> IArrayParamSetter<'Arg>

module PgArrayParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<unit, MultipleArrays>

    type SimpleBuilder() =

        let getNpgSqlDbType t = 
            if t = typeof<int> then NpgsqlDbType.Integer
            elif t = typeof<Int64> then NpgsqlDbType.Bigint
            elif t = typeof<Int16> then NpgsqlDbType.Smallint
            elif t = typeof<bool> then NpgsqlDbType.Boolean
            elif t = typeof<decimal> then NpgsqlDbType.Numeric
            elif t = typeof<DateTime> then NpgsqlDbType.Timestamp
            elif t = typeof<TimeSpan> then NpgsqlDbType.Interval
            elif t = typeof<Guid> then NpgsqlDbType.Uuid
            elif t = typeof<char> then NpgsqlDbType.Char
            elif t = typeof<string> then NpgsqlDbType.Varchar
            elif t = typeof<double> then NpgsqlDbType.Double
            elif t = typeof<byte[]> then NpgsqlDbType.Bytea
            else failwith <| sprintf "Unmappable type: %O" t

        let getArray(arrays: MultipleArrays, name: string): 'Item array = 
            match arrays.Data.TryGetValue(name) with
            | true, (_, array) -> 
                array :?> 'Item array
            | false, _ ->
                let array = Array.zeroCreate arrays.ArraySize
                arrays.Data.Add(name, (getNpgSqlDbType typeof<'Item>, array))
                array

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isSimpleType(argType)

            member this.Build<'Arg> (name: string, _, _: unit) = 
                { new IArrayParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, arrays: MultipleArrays) = 
                        let array = getArray(arrays, name)
                        array[index.Value] <- value
                    member __.SetNull(index: int option, arrays: MultipleArrays) = 
                        match index with
                        | None -> arrays.Data[name] <- (getNpgSqlDbType typeof<'Arg>), DBNull.Value
                        | Some _ -> failwithf "Null array items are not allowed: %s: %A" name typeof<'Arg>
                    member __.SetArtificial(_: int option, arrays: MultipleArrays) = 
                        let array = getArray(arrays, name)
                        array[0] <- this.GetArtificialValue<'Arg>() :?> 'Arg
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()

/// <summary>
/// Provides methods creating various Postgres array parameter builders.
/// </summary>
type PgArrayParams() = 
    inherit GenericSetters.GenericSetterBuilder<unit, MultipleArrays>()
           
/// <summary>
/// The field-to-array mapping override.
/// </summary>
type PgArrayParamOverride<'Arg> = GenericSetters.Override<unit, MultipleArrays, 'Arg>

