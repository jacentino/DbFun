namespace DbFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open DbFun.Core

type IRowGetter<'Result> = GenericGetters.IGetter<IDataRecord, 'Result>

type IRowGetterProvider = GenericGetters.IGetterProvider<IDataRecord, IDataRecord>

type BuildRowGetter<'Arg> = GenericGetters.BuildGetter<IDataRecord, IDataRecord, 'Arg>

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
                    
            member this.Build(name: string, _, prototype: IDataRecord): IRowGetter<'Result> = 
                let ordinal = 
                    try
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
                let getter = Expression.Lambda<Func<IDataRecord, 'Result>>(convertedCall, recParam).Compile()
                { new IRowGetter<'Result> with
                        member __.Get(record: IDataRecord): 'Result = 
                            getter.Invoke(record)
                        member __.IsNull(record: IDataRecord): bool = 
                            record.IsDBNull(ordinal)
                        member this.Create(arg1: IDataRecord): unit = 
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
    static member Key<'Result1, 'Result2>(createGetter1: BuildRowGetter<'Result1>, createGetter2: BuildRowGetter<'Result2>)
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
    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, resultName: string) = 
        Rows.Tuple(Rows.Key<'Primary, 'Foreign>(primaryName, foreignName), Rows.Simple<'Result>(resultName))

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
    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: BuildRowGetter<'Result>) = 
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
    static member Keyed<'Primary, 'Foreign, 'Result>(primary: BuildRowGetter<'Primary>, foreign: BuildRowGetter<'Foreign>, result: BuildRowGetter<'Result>) = 
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
    static member PKeyed<'Primary, 'Result>(primaryName: string, resultName: string) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primaryName, ""), Rows.Simple<'Result>(resultName))

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primaryName: string, result: BuildRowGetter<'Result>) = 
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
    static member PKeyed<'Primary, 'Result>(primary: BuildRowGetter<'Primary>, result: BuildRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<'Primary, unit>(primary, Rows.Simple<unit>("")), result)

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreignName">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreignName: string, resultName: string) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>("", foreignName), Rows.Simple<'Result>(resultName))

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreignName">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreignName: string, result: BuildRowGetter<'Result>) = 
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
    static member FKeyed<'Foreign, 'Result>(foreign: BuildRowGetter<'Foreign>, result: BuildRowGetter<'Result>) = 
        Rows.Tuple(Rows.Key<unit, 'Foreign>(Rows.Simple<unit>(""), foreign), result)

/// <summary>
/// The column-to-field mapping override.
/// </summary>
type RowOverride<'Arg> = GenericGetters.Override<IDataRecord, IDataRecord, 'Arg>