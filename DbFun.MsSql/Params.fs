namespace DbFun.MsSql.Builders

open System
open System.Data
open FSharp.Reflection
open DbFun.Core
open DbFun.Core.Builders
open Microsoft.Data.SqlClient.Server
open Microsoft.Data.SqlClient

module ParamsImpl = 

    type TVPCollectionBuilder(createConnection: unit -> IDbConnection, tvpProvider: ITVParamSetterProvider) = 

        let createMetaData(name: string, typeName: string, maxLen: int64, precision: byte, scale: byte) = 
            let dbType = Enum.Parse(typeof<SqlDbType>, typeName, true) :?> SqlDbType
            match dbType with
            | SqlDbType.Char 
            | SqlDbType.NChar 
            | SqlDbType.VarChar 
            | SqlDbType.NVarChar -> SqlMetaData(name, dbType, Math.Min(maxLen, 4000L))
            | SqlDbType.Binary 
            | SqlDbType.VarBinary -> SqlMetaData(name, dbType, maxLen)
            | SqlDbType.Decimal 
            | SqlDbType.Money 
            | SqlDbType.SmallMoney -> SqlMetaData(name, dbType, precision, scale)
            | _ -> SqlMetaData(name, dbType)

        let getRecSequence(metadata: SqlMetaData array, setter: ITVParamSetter<'ItemType>, data: 'ItemType seq): SqlDataRecord seq = 
            let record = SqlDataRecord(metadata)
            seq {
                for item in data do
                    setter.SetValue(item, record)
                    yield record
            }

        let getMetaData (tvpName: string) =
            use connection = createConnection()
            connection.Open()
            use command = connection.CreateCommand()
            command.CommandText <- "select c.name, t.name as typeName, c.max_length, c.precision, c.scale, c.is_nullable
                                    from sys.table_types tt 
	                                    join sys.columns c on c.object_id = tt.type_table_object_id
	                                    join sys.types t on t.system_type_id = c.system_type_id and t.user_type_id = c.user_type_id
                                    where tt.name = @name"
            let param = command.CreateParameter()
            param.ParameterName <- "@name"
            param.Value <- tvpName
            command.Parameters.Add(param) |> ignore
            use reader = command.ExecuteReader()
            [| while reader.Read() do
                yield createMetaData(reader.GetString 0, reader.GetString 1, int64(reader.GetInt16 2), reader.GetByte 3, reader.GetByte  4)
            |]

        let convertToListSetter (seqSetter: IParamSetter<'ItemType seq>) : IParamSetter<'ItemType list> = 
            { new IParamSetter<'ItemType list> with
                  member this.SetArtificial(command: IDbCommand): unit = 
                      seqSetter.SetArtificial(command)
                  member this.SetNull(command: IDbCommand): unit = 
                      seqSetter.SetNull(command)
                  member this.SetValue(value: 'ItemType list, command: IDbCommand): unit = 
                      seqSetter.SetValue(value, command)
            }

        let convertToArraySetter (seqSetter: IParamSetter<'ItemType seq>) : IParamSetter<'ItemType array> = 
            { new IParamSetter<'ItemType array> with
                  member this.SetArtificial(command: IDbCommand): unit = 
                      seqSetter.SetArtificial(command)
                  member this.SetNull(command: IDbCommand): unit = 
                      seqSetter.SetNull(command)
                  member this.SetValue(value: 'ItemType array, command: IDbCommand): unit = 
                      seqSetter.SetValue(value, command)
            }

        member this.CreateSeqSetter(name: string, tvpName: string option): IParamSetter<'ItemType seq> = 
            let setter = fun (provider: ITVParamSetterProvider, prototype) -> provider.Setter<'ItemType>(name, prototype) 
            this.CreateSeqSetter(setter, name, tvpName)

        member this.CreateListSetter(name: string, tvpName: string option): IParamSetter<'ItemType list> = 
            let seqSetter = this.CreateSeqSetter(name, tvpName)
            convertToListSetter seqSetter

        member this.CreateArraySetter(name: string, tvpName: string option): IParamSetter<'ItemType array> = 
            let seqSetter = this.CreateSeqSetter(name, tvpName)
            convertToArraySetter seqSetter

        member __.CreateSeqSetter(createRecordSetter: ITVParamSetterProvider * SqlDataRecord -> ITVParamSetter<'ItemType>, name: string, tvpName: string option): IParamSetter<'ItemType seq> =            
            let tvpName = tvpName |> Option.defaultValue typeof<'ItemType>.Name
            let metadata = getMetaData(tvpName)
            let recordSetter = createRecordSetter(tvpProvider, SqlDataRecord(metadata))
            let toSqlDataRecords = fun (data: 'ItemType seq) -> getRecSequence(metadata, recordSetter, data)
            let record = SqlDataRecord(metadata)
            let artificialValues = 
                seq {
                    recordSetter.SetArtificial(record)
                    yield record
                }
            { new IParamSetter<'ItemType seq> with
                member __.SetValue (value: 'ItemType seq, command: IDbCommand) = 
                    let param = command.CreateParameter() :?> SqlParameter
                    param.ParameterName <- name
                    param.SqlDbType <- SqlDbType.Structured
                    param.TypeName <- tvpName
                    param.Value <- toSqlDataRecords value
                    command.Parameters.Add param |> ignore
                member __.SetNull(command: IDbCommand) = 
                    let param = command.CreateParameter()
                    param.ParameterName <- name
                    param.Value <- DBNull.Value
                    command.Parameters.Add param |> ignore
                member __.SetArtificial(command: IDbCommand) = 
                    let param = command.CreateParameter()
                    param.ParameterName <- name
                    param.Value <- artificialValues
                    command.Parameters.Add param |> ignore                    
            }

        member this.CreateListSetter(setterBuilder: ITVParamSetterProvider * SqlDataRecord -> ITVParamSetter<'ItemType>, name: string, tvpName: string option): IParamSetter<'ItemType list> =
            let seqSetter = this.CreateSeqSetter(setterBuilder, name, tvpName)
            convertToListSetter seqSetter 

        member this.CreateArraySetter(setterBuilder: ITVParamSetterProvider * SqlDataRecord -> ITVParamSetter<'ItemType>, name: string, tvpName: string option): IParamSetter<'ItemType array> =
            let seqSetter = this.CreateSeqSetter(setterBuilder, name, tvpName)
            convertToArraySetter seqSetter

        interface ParamsImpl.IBuilder with

            member __.CanBuild(argType: Type): bool = 
                Types.isCollectionType argType && (FSharpType.IsRecord (Types.getElementType argType) || FSharpType.IsTuple (Types.getElementType argType))

            member this.Build(name: string, _: IParamSetterProvider, _: unit): IParamSetter<'Arg> = 
                let elemType = Types.getElementType typeof<'Arg>
                let createSetterName = 
                    if typeof<'Arg>.IsArray then "CreateArraySetter"
                    elif typedefof<'Arg> = typedefof<list<_>> then "CreateListSetter"
                    elif typedefof<'Arg> = typedefof<seq<_>> then "CreateSeqSetter"
                    else failwithf "Unsupported collection type: %s" typedefof<'Arg>.Name
                let createSetterMethod = this.GetType().GetMethod(createSetterName, [| typeof<string>; typeof<string option> |]).MakeGenericMethod(elemType)
                let setter = createSetterMethod.Invoke(this, [| name; None |]) :?> IParamSetter<'Arg>
                setter

    type BaseSetterProvider = GenericSetters.BaseSetterProvider<SqlDataRecord, SqlDataRecord>

    type DerivedSetterProvider = GenericSetters.DerivedSetterProvider<SqlDataRecord, SqlDataRecord>

    type UnitBuilder = GenericSetters.UnitBuilder<SqlDataRecord, SqlDataRecord>

    type SequenceBuilder = GenericSetters.SequenceBuilder<SqlDataRecord, SqlDataRecord>

    type Converter<'Source, 'Target> = GenericSetters.Converter<SqlDataRecord, SqlDataRecord, 'Source, 'Target>

    type SeqItemConverter<'Source, 'Target> = GenericSetters.SeqItemConverter<SqlDataRecord, SqlDataRecord, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericSetters.EnumConverter<SqlDataRecord, SqlDataRecord, 'Underlying>

    type EnumSeqConverter<'Underlying> = GenericSetters.EnumSeqConverter<SqlDataRecord, SqlDataRecord, 'Underlying>

    type AttrEnumSeqConverter = GenericSetters.AttrEnumSeqConverter<SqlDataRecord, SqlDataRecord>

    type AttrEnumConverter = GenericSetters.AttrEnumConverter<SqlDataRecord, SqlDataRecord>

    type OptionBuilder = GenericSetters.OptionBuilder<SqlDataRecord, SqlDataRecord>

    type RecordBuilder = GenericSetters.RecordBuilder<SqlDataRecord, SqlDataRecord>

    type TupleBuilder = GenericSetters.TupleBuilder<SqlDataRecord, SqlDataRecord>



    let getDefaultBuilders(createConnection: unit -> IDbConnection): ParamsImpl.IBuilder list = 
        let tvpProvider = GenericSetters.BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders())
        [ TVPCollectionBuilder(createConnection, tvpProvider) ] @ ParamsImpl.getDefaultBuilders()

open ParamsImpl

/// <summary>
/// Provides methods creating various query parameter builders.
/// </summary>
type Params() = 
    inherit Builders.Params()
        
    static member GetTvpBuilder<'Arg>(provider: IParamSetterProvider) = 
        match provider.Builder(typeof<'Arg>) with
        | Some builder -> builder :?> TVPCollectionBuilder
        | None -> failwithf "Builder not found for type %s" typeof<'Arg>.Name

    /// <summary>
    /// Creates a table-valued builder for a sequence of values (records or tuples).
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="tvpName">
    /// The name of user-defined table type representing records passed in the parameter.
    /// </param>
    static member TableValuedSeq<'Record>(name: string, ?tvpName: string): BuildParamSetter<'Record seq> =
        fun (provider, _) -> Params.GetTvpBuilder<'Record seq>(provider).CreateSeqSetter(name, tvpName)

    /// <summary>
    /// Creates a table-valued builder for a list of values (records or tuples).
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="tvpName">
    /// The name of user-defined table type representing records passed in the parameter.
    /// </param>
    static member TableValuedList<'Record>(name: string, ?tvpName: string): BuildParamSetter<'Record list> =
        fun (provider, _) -> Params.GetTvpBuilder<'Record list>(provider).CreateListSetter(name, tvpName)

    /// <summary>
    /// Creates a table-valued builder for an array of values (records or tuples).
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="tvpName">
    /// The name of user-defined table type representing records passed in the parameter.
    /// </param>
    static member TableValuedArray<'Record>(name: string, ?tvpName: string): BuildParamSetter<'Record array> =
        fun (provider, _) -> Params.GetTvpBuilder<'Record array>(provider).CreateArraySetter(name, tvpName)

    /// <summary>
    /// Creates a table-valued builder for a sequence of values (records or tuples).
    /// </summary>
    /// <param name="createTvpSetter">
    /// The table-valued parameter builder.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="tvpName">
    /// The name of user-defined table type representing records passed in the parameter.
    /// </param>
    static member TableValuedSeq<'Record>(createTvpSetter: BuildTVParamSetter<'Record>, name: string, ?tvpName: string): BuildParamSetter<'Record seq> =
        fun (provider, _) -> Params.GetTvpBuilder<'Record seq>(provider).CreateSeqSetter(createTvpSetter, name, tvpName)

    /// <summary>
    /// Creates a table-valued builder for a list of values (records or tuples).
    /// </summary>
    /// <param name="createTvpSetter">
    /// The table-valued parameter builder.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="tvpName">
    /// The name of user-defined table type representing records passed in the parameter.
    /// </param>
    static member TableValuedList<'Record>(createTvpSetter: BuildTVParamSetter<'Record>, name: string, ?tvpName: string): BuildParamSetter<'Record list> =
        fun (provider, _) -> Params.GetTvpBuilder<'Record list>(provider).CreateListSetter(createTvpSetter, name, tvpName)

    /// <summary>
    /// Creates a table-valued builder for an array of values (records or tuples).
    /// </summary>
    /// <param name="createTvpSetter">
    /// The table-valued parameter builder.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="tvpName">
    /// The name of user-defined table type representing records passed in the parameter.
    /// </param>
    static member TableValuedArray<'Record>(createTvpSetter: BuildTVParamSetter<'Record>, name: string, ?tvpName: string): BuildParamSetter<'Record array> =
        fun (provider, _) -> Params.GetTvpBuilder<'Record array>(provider).CreateArraySetter(createTvpSetter, name, tvpName)
