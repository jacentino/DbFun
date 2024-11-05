namespace DbFun.OracleManaged.Builders

open System
open System.Collections.Generic
open DbFun.Core
open DbFun.Core.Builders
open Oracle.ManagedDataAccess.Client

type MultipleArrays = 
    {
        ArraySize   : int
        Data        : IDictionary<string, OracleDbType * obj>
    }

type IArrayParamSetter<'Arg> = GenericSetters.ISetter<MultipleArrays, 'Arg>

type IArrayParamSetterProvider = GenericSetters.ISetterProvider<unit, MultipleArrays>

type ArrayParamSpecifier<'Arg> = IArrayParamSetterProvider * unit -> IArrayParamSetter<'Arg>

module OracleArrayParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<unit, MultipleArrays>

    type SimpleBuilder() =

        let getOracleDbType t = 
            if t = typeof<int> then OracleDbType.Int32
            elif t = typeof<Int64> then OracleDbType.Int64
            elif t = typeof<Int16> then OracleDbType.Int16
            elif t = typeof<bool> then OracleDbType.Boolean
            elif t = typeof<decimal> then OracleDbType.Decimal
            elif t = typeof<DateTime> then OracleDbType.TimeStamp
            //elif t = typeof<TimeSpan> then OracleDbType.IntervalDS // TODO: maybe OracleDbType.IntervalYM
            //elif t = typeof<Guid> then OracleDbType.
            elif t = typeof<char> then OracleDbType.Char
            elif t = typeof<string> then OracleDbType.Varchar2
            elif t = typeof<double> then OracleDbType.Double
            elif t = typeof<byte[]> then OracleDbType.Vector_Binary
            else failwith <| sprintf "Unmappable type: %O" t

        let getArray(arrays: MultipleArrays, name: string): 'Item array = 
            match arrays.Data.TryGetValue(name) with
            | true, (_, array) -> 
                array :?> 'Item array
            | false, _ ->
                let array = Array.zeroCreate arrays.ArraySize
                arrays.Data.Add(name, (getOracleDbType typeof<'Item>, array))
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
                        | None -> arrays.Data[name] <- (getOracleDbType typeof<'Arg>), DBNull.Value
                        | Some _ -> 
                            let array = getArray(arrays, name)
                            array[index.Value] <- Unchecked.defaultof<'Arg>
                    member __.SetArtificial(_: int option, arrays: MultipleArrays) = 
                        let array = getArray(arrays, name)
                        array[0] <- this.GetArtificialValue<'Arg>() :?> 'Arg
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()

/// <summary>
/// Provides methods creating various Oracle array parameter builders.
/// </summary>
type OracleArrayParams() = 
    inherit GenericSetters.GenericSetterBuilder<unit, MultipleArrays>()
           
/// <summary>
/// The field-to-array mapping override.
/// </summary>
type OracleArrayParamOverride<'Arg> = GenericSetters.Override<unit, MultipleArrays, 'Arg>

