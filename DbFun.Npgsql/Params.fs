namespace DbFun.Npgsql.Builders

open System
open System.Collections.Generic
open System.Data
open DbFun.Core
open DbFun.Core.Builders
open Npgsql
open NpgsqlTypes

module ParamsImpl =

    type PgArrayBuilder(arrayProvider: IArrayParamSetterProvider) =

        static let setParameters(arrays: MultipleArrays, command: IDbCommand) = 
            for kv in arrays.Data do
                let dbType, value = kv.Value
                let param = new NpgsqlParameter()
                param.ParameterName <- kv.Key
                param.Value <- value
                param.NpgsqlDbType <- NpgsqlDbType.Array ||| dbType
                command.Parameters.Add(param) |> ignore

        static member CreateParamSetter(itemSetter: IArrayParamSetter<'Item>, getLength: 'Collection -> int, populate: MultipleArrays -> 'Collection -> unit) = 
            { new IParamSetter<'Collection> with
                member __.SetValue (items: 'Collection, _: int option, command: IDbCommand) = 
                    let arrays = { ArraySize = getLength items; Data = Dictionary<string, NpgsqlDbType * obj>() }
                    populate arrays items
                    setParameters(arrays, command)
                member __.SetNull(_: int option, command: IDbCommand) = 
                    let arrays = { ArraySize = 1; Data = Dictionary<string, NpgsqlDbType * obj>() }
                    itemSetter.SetNull(None, arrays)
                    setParameters(arrays, command)
                member __.SetArtificial(_: int option, command: IDbCommand) = 
                    let arrays = { ArraySize = 1; Data = Dictionary<string, NpgsqlDbType * obj>() }
                    itemSetter.SetArtificial(Some 0, arrays)
                    setParameters(arrays, command)
            }

        member __.CreateSeqSetter<'Item>(name: string) = 
            let itemSetter = arrayProvider.Setter<'Item>(name, ())
            PgArrayBuilder.CreateParamSetter(itemSetter, Seq.length, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateListSetter<'Item>(name: string) = 
            let itemSetter = arrayProvider.Setter<'Item>(name, ())
            PgArrayBuilder.CreateParamSetter(itemSetter, List.length, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateArraySetter<'Item>(name: string) = 
            let itemSetter = arrayProvider.Setter<'Item>(name, ())
            PgArrayBuilder.CreateParamSetter(itemSetter, Array.length, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateSeqSetter<'Item>(itemSpecifier: ArrayParamSpecifier<'Item>) = 
            let itemSetter = itemSpecifier(arrayProvider, ())
            PgArrayBuilder.CreateParamSetter(itemSetter, Seq.length, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateListSetter<'Item>(itemSpecifier: ArrayParamSpecifier<'Item>) = 
            let itemSetter = itemSpecifier(arrayProvider, ())
            PgArrayBuilder.CreateParamSetter(itemSetter, List.length, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateArraySetter<'Item>(itemSpecifier: ArrayParamSpecifier<'Item>) = 
            let itemSetter = itemSpecifier(arrayProvider, ())
            PgArrayBuilder.CreateParamSetter(itemSetter, Array.length, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        interface ParamsImpl.IBuilder with

            member __.CanBuild (argType: Type) = Types.isCollectionType argType 

            member this.Build<'Arg> (name: string, _: IParamSetterProvider, _: unit) = 
                let itemType = Types.getElementType typeof<'Arg>
                let setterName = 
                    if typeof<'Arg>.IsArray then "CreateArraySetter"
                    elif typedefof<'Arg> = typedefof<list<_>> then "CreateListSetter"
                    else "CreateSeqSetter"
                let createSetterMethod = this.GetType().GetMethod(setterName, [| typeof<string> |]).MakeGenericMethod(itemType)
                createSetterMethod.Invoke(this, [| name |]) :?> IParamSetter<'Arg>
    
    type BaseSetterProvider = GenericSetters.BaseSetterProvider<unit, MultipleArrays>

    type InitialDerivedSetterProvider<'Config> = GenericSetters.InitialDerivedSetterProvider<unit, MultipleArrays, 'Config>

    type DerivedSetterProvider<'Config> = GenericSetters.DerivedSetterProvider<unit, MultipleArrays, 'Config>

    type UnitBuilder = GenericSetters.UnitBuilder<unit, MultipleArrays>

    type SequenceBuilder = GenericSetters.SequenceBuilder<unit, MultipleArrays>

    type Converter<'Source, 'Target> = GenericSetters.Converter<unit, MultipleArrays, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericSetters.EnumConverter<unit, MultipleArrays, 'Underlying>

    type UnionBuilder = GenericSetters.UnionBuilder<unit, MultipleArrays>

    type OptionBuilder = GenericSetters.OptionBuilder<unit, MultipleArrays>

    type RecordBuilder = GenericSetters.RecordBuilder<unit, MultipleArrays>

    type TupleBuilder = GenericSetters.TupleBuilder<unit, MultipleArrays>

    type Configurator<'Config> = GenericSetters.Configurator<unit, MultipleArrays, 'Config>


open ParamsImpl

/// <summary>
/// Provides methods creating various query parameter builders.
/// </summary>
type Params() = 
    inherit Builders.Params()
        
    static member GetPgArrayBuilder<'Arg>(provider: IParamSetterProvider) = 
        match provider.Builder(typeof<'Arg>) with
        | Some builder -> 
            try
                builder :?> PgArrayBuilder
            with ex ->
                reraise()
        | None -> failwithf "Builder not found for type %s" typeof<'Arg>.Name

    /// <summary>
    /// Creates a builder for a sequence of values.
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    static member PgSeq<'Record>(?name: string): ParamSpecifier<'Record seq> =
        fun (provider, _) -> Params.GetPgArrayBuilder<'Record seq>(provider).CreateSeqSetter(defaultArg name "")

    /// <summary>
    /// Creates a builder builder for a list of values.
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    static member PgList<'Record>(?name: string): ParamSpecifier<'Record list> =
        fun (provider, _) -> Params.GetPgArrayBuilder<'Record list>(provider).CreateListSetter(defaultArg name "")

    /// <summary>
    /// Creates a builder for an array of values.
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    static member PgArray<'Record>(?name: string): ParamSpecifier<'Record array> =
        fun (provider, _) -> Params.GetPgArrayBuilder<'Record array>(provider).CreateArraySetter(defaultArg name "")

    /// <summary>
    /// Creates a builder for a sequence of values.
    /// </summary>
    /// <param name="arraySpecifier">
    /// The array parameter builder.
    /// </param>
    static member PgSeq<'Record>(arraySpecifier: ArrayParamSpecifier<'Record>): ParamSpecifier<'Record seq> =
        fun (provider, _) -> Params.GetPgArrayBuilder<'Record seq>(provider).CreateSeqSetter(arraySpecifier)

    /// <summary>
    /// Creates a builder for a list of values.
    /// </summary>
    /// <param name="arraySpecifier">
    /// The array parameter builder.
    /// </param>
    static member PgList<'Record>(arraySpecifier: ArrayParamSpecifier<'Record>): ParamSpecifier<'Record list> =
        fun (provider, _) -> Params.GetPgArrayBuilder<'Record list>(provider).CreateListSetter(arraySpecifier)

    /// <summary>
    /// Creates a builder for an array of values.
    /// </summary>
    /// <param name="arraySpecifier">
    /// The array parameter builder.
    /// </param>
    static member PgArray<'Record>(arraySpecifier: ArrayParamSpecifier<'Record>, ?name: string): ParamSpecifier<'Record array> =
        fun (provider, _) -> Params.GetPgArrayBuilder<'Record array>(provider).CreateArraySetter(arraySpecifier)
