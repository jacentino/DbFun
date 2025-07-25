﻿namespace DbFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open DbFun.Core

type IRowGetter<'Result> = GenericGetters.IGetter<IDataRecord, 'Result>

type IRowGetterProvider = GenericGetters.IGetterProvider<IDataRecord, IDataRecord>

type RowSpecifier<'Arg> = GenericGetters.GetterSpecifier<IDataRecord, IDataRecord, 'Arg>

module RowsImpl = 

    type IBuilder = GenericGetters.IBuilder<IDataRecord, IDataRecord>


    type SimpleColumnBuilder() = 

        static let typedColAccessMethods = 
            [
                typeof<Boolean>,    "GetBoolean"
                typeof<Byte>,       "GetByte"
                typeof<Char>,       "GetChar"
                typeof<DateTime>,   "GetDateTime"
                typeof<Decimal>,    "GetDecimal"
                typeof<Double>,     "GetDouble"
                typeof<float>,      "GetFloat"
                typeof<Guid>,       "GetGuid"
                typeof<Int16>,      "GetInt16"
                typeof<Int32>,      "GetInt32"
                typeof<Int64>,      "GetInt64" 
                typeof<string>,     "GetString"
            ] 
            |> List.map (fun (t, name) -> t, typeof<IDataRecord>.GetMethod(name))                

        static let getValueMethod = typeof<IDataRecord>.GetMethod("GetValue")

        member __.GetChar(value: string) = value.[0]

        interface IBuilder with
                
            member __.CanBuild(argType: Type): bool = Types.isSimpleType argType
                    
            member this.Build(name: string, provider, prototype: IDataRecord): IRowGetter<'Result> = 
                let ordinal = 
                    try
                        if String.IsNullOrEmpty(name) && prototype.FieldCount = 1 then
                            0
                        else
                            prototype.GetOrdinal(name)
                    with ex -> raise <| Exception(sprintf "Column doesn't exist: %s" name, ex)
                let fieldType = prototype.GetFieldType(ordinal)
                let colGetter = typedColAccessMethods |> List.tryFind (fst >> (=) fieldType) |> Option.map snd |> Option.defaultValue getValueMethod
                let recParam = Expression.Parameter(typeof<IDataRecord>)
                let call = Expression.Call(recParam, colGetter, Expression.Constant(ordinal))
                let convertedCall = 
                    if typeof<'Result> = typeof<char> && call.Type = typeof<string> then
                        let getCharMethod = this.GetType().GetMethod("GetChar")
                        Expression.Call(Expression.Constant(this), getCharMethod, call) :> Expression
                    elif typeof<'Result> <> call.Type then
                        try
                            Expression.Convert(call, typeof<'Result>) :> Expression
                        with :? InvalidOperationException as ex ->
                            raise <| Exception(sprintf "Column type doesn't match field type: %s (%s -> %s)" name call.Type.Name typeof<'Result>.Name, ex)
                    else
                        call :> Expression
                let getter = provider.Compiler.Compile<Func<IDataRecord, 'Result>>(convertedCall, recParam)
                { new IRowGetter<'Result> with
                    member __.Get(record: IDataRecord): 'Result = 
                        getter.Invoke(record)
                    member __.IsNull(record: IDataRecord): bool = 
                        record.IsDBNull(ordinal)
                    member this.Create(_: IDataRecord): unit = 
                        raise (System.NotImplementedException())
                }


    type NoPrototypeColumnBuilder() = 

        let getOrdinal(record: IDataRecord, name: string) = 
            try
                if String.IsNullOrEmpty(name) && record.FieldCount = 1 then 0 else record.GetOrdinal(name)
            with ex -> raise <| Exception(sprintf "Column doesn't exist: %s" name, ex)

        interface IBuilder with
                
            member __.CanBuild(argType: Type): bool = Types.isSimpleType argType
                    
            member this.Build(name: string, _, _: IDataRecord): IRowGetter<'Result> = 
                { new IRowGetter<'Result> with
                    member __.Get(record: IDataRecord): 'Result = 
                        let ordinal = getOrdinal(record, name)
                        let fieldType = record.GetFieldType(ordinal)
                        let value = record.GetValue(ordinal)
                        if typeof<'Result> = typeof<char> && fieldType = typeof<string> then
                            (value :?> string).[0] |> box :?> 'Result
                        elif typeof<'Result> <> fieldType then
                            Convert.ChangeType(value, typeof<'Result>) :?> 'Result
                        else    
                            value :?> 'Result
                    member __.IsNull(record: IDataRecord): bool = 
                        record.IsDBNull(getOrdinal(record, name))
                    member this.Create(_: IDataRecord): unit = 
                        raise (System.NotImplementedException())
                }
    

    type KeySpecifier<'Primary, 'Foreign>(primary: 'Primary, foreign: 'Foreign) =
            member __.Primary = primary
            member __.Foreign = foreign

    type BaseGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>

    type InitialDerivedGetterProvider<'Config> = GenericGetters.InitialDerivedGetterProvider<IDataRecord, IDataRecord, 'Config>

    type DerivedGetterProvider<'Config> = GenericGetters.DerivedGetterProvider<IDataRecord, IDataRecord, 'Config>

    type UnitBuilder = GenericGetters.UnitBuilder<IDataRecord, IDataRecord>

    type SequenceBuilder = GenericGetters.SequenceBuilder<IDataRecord, IDataRecord>

    type OptionBuilder = GenericGetters.OptionBuilder<IDataRecord, IDataRecord>

    type Converter<'Source, 'Target> = GenericGetters.Converter<IDataRecord, IDataRecord, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericGetters.EnumConverter<IDataRecord, IDataRecord, 'Underlying>

    type UnionBuilder = GenericGetters.UnionBuilder<IDataRecord, IDataRecord>

    type RecordBuilder = GenericGetters.RecordBuilder<IDataRecord, IDataRecord>

    type TupleBuilder = GenericGetters.TupleBuilder<IDataRecord, IDataRecord>

    type Configurator<'Config> = GenericGetters.Configurator<IDataRecord, IDataRecord, 'Config>

    let getDefaultBuilders(): IBuilder list = 
        SimpleColumnBuilder() :: GenericGetters.getDefaultBuilders()

open RowsImpl

/// <summary>
/// Provides methods creating various row/column mapping builders.
/// </summary>
type Rows() = 
    inherit GenericGetters.GenericGetterBuilder<IDataRecord, IDataRecord>()

    /// <summary>
    /// Creates a key specifier builder.
    /// </summary>
    /// <param name="name1">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="name2">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    static member Key<'Result1, 'Result2>(name1: string, name2: string): IRowGetterProvider * IDataRecord -> IRowGetter<KeySpecifier<'Result1, 'Result2>> =
        fun (provider: IRowGetterProvider, prototype: IDataRecord) ->
            let getter1 = provider.Getter<'Result1>(name1, prototype)
            let getter2 = provider.Getter<'Result2>(name2, prototype)
            { new IRowGetter<KeySpecifier<'Result1, 'Result2>> with
                member __.IsNull(record: IDataRecord): bool = 
                    getter1.IsNull(record) && getter2.IsNull(record)
                member __.Get(record: IDataRecord): KeySpecifier<'Result1, 'Result2> = 
                    KeySpecifier(getter1.Get(record), getter2.Get(record))
                member __.Create(record: IDataRecord): unit = 
                    getter1.Create(record)
                    getter2.Create(record)
            }

    /// <summary>
    /// Creates a key specifier builder.
    /// </summary>
    /// <param name="createGetter1">
    /// The primary key builder.
    /// </param>
    /// <param name="createGetter2">
    /// The foreign key builder.
    /// </param>
    static member Key<'Result1, 'Result2>(createGetter1: RowSpecifier<'Result1>, createGetter2: RowSpecifier<'Result2>)
            : IRowGetterProvider * IDataRecord -> IRowGetter<KeySpecifier<'Result1, 'Result2>> = 
        fun (provider: IRowGetterProvider, prototype: IDataRecord) ->
            let getter1 = createGetter1(provider, prototype)
            let getter2 = createGetter2(provider, prototype)
            { new IRowGetter<KeySpecifier<'Result1, 'Result2>> with
                member __.IsNull(record: IDataRecord): bool = 
                    getter1.IsNull(record) && getter2.IsNull(record)
                member __.Get(record: IDataRecord): KeySpecifier<'Result1, 'Result2> = 
                    KeySpecifier(getter1.Get(record), getter2.Get(record))
                member __.Create(record: IDataRecord): unit = 
                    getter1.Create(record)
                    getter2.Create(record)
            }

    /// <summary>
    /// Creates a builder of result with primary and foreign key, that can be used in result joins as a master, as well as a detail result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="foreignName"></param>
    /// The name of the foreign key column or record prefix for compound keys.
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, ?resultName: string) = 
        Rows.Tuple(Rows.Key<'Primary, 'Foreign>(primaryName, foreignName), Rows.Auto<'Result>(?name = resultName))

    /// <summary>
    /// Creates a builder of result with primary and foreign key, that can be used in result joins as a master, as well as a detail result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="foreignName"></param>
    /// The name of the foreign key column or record prefix for compound keys.
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: RowSpecifier<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, 'Foreign>(primaryName, foreignName), result)

    /// <summary>
    /// Creates a builder of result with primary and foreign key, that can be used in result joins as a master, as well as a detail result.
    /// </summary>
    /// <param name="primary">
    /// The primary key builder.
    /// </param>
    /// <param name="foreign">
    /// The foreign key builder.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member Keyed<'Primary, 'Foreign, 'Result>(primary: RowSpecifier<'Primary>, foreign: RowSpecifier<'Foreign>, result: RowSpecifier<'Result>) = 
        Rows.Tuple(Rows.Key(primary, foreign), result)

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primaryName: string, ?resultName: string) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primaryName, ""), Rows.Auto<'Result>(?name = resultName))

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primaryName: string, result: RowSpecifier<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primaryName, ""), result)

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primary">
    /// The primary key builder.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primary: RowSpecifier<'Primary>, result: RowSpecifier<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primary, Rows.Auto()), result)

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreignName">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreignName: string, ?resultName: string) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>("", foreignName), Rows.Auto<'Result>(?name = resultName))

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreignName">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreignName: string, result: RowSpecifier<'Result>) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>("", foreignName), result)

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreign">
    /// The foreign key builder.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreign: RowSpecifier<'Foreign>, result: RowSpecifier<'Result>) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>(Rows.Auto<unit>(), foreign), result)

/// <summary>
/// The column-to-field mapping override.
/// </summary>
type RowOverride<'Arg> = GenericGetters.Override<IDataRecord, IDataRecord, 'Arg>