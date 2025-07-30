namespace DbFun.Firebird.Builders

open DbFun.Core.Builders
open FirebirdSql.Data.FirebirdClient
open System
open DbFun.Core
open DbFun.Core.Builders.Compilers
open System.Data


type IBatchParamSetter<'Arg> = GenericSetters.ISetter<FbParameterCollection, 'Arg>

type IBatchParamSetterProvider = GenericSetters.ISetterProvider<unit, FbParameterCollection>

type BatchParamSpecifier<'Arg> = IBatchParamSetterProvider * unit -> IBatchParamSetter<'Arg>

module BatchParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<unit, FbParameterCollection>

    type SimpleBuilder() = 

        member __.FindOrCreateParam(batchParams: FbParameterCollection, name: string) = 
            let index = batchParams.IndexOf(name)
            if index = -1 then
                let param = FbParameter()
                param.ParameterName <- name
                batchParams.Add param |> ignore
                param
            else
                batchParams.[index]

        member __.Update(param: IDbDataParameter, value: obj) = 
            if param.Value = null || param.Value = DBNull.Value then
                param.Value <- value
            else
                failwithf "Duplicate parameter definition: %s" param.ParameterName

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isSimpleType(argType)

            member this.Build<'Arg> (name: string, _, ()) = 
                { new IBatchParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, batchParams: FbParameterCollection) = 
                        let param = this.FindOrCreateParam(batchParams, name)
                        this.Update(param, value)
                    member __.SetNull(index: int option, batchParams: FbParameterCollection) = 
                        let param = this.FindOrCreateParam(batchParams, name)
                        this.Update(param, DBNull.Value)
                    member __.SetArtificial(index: int option, batchParams: FbParameterCollection) = 
                        let param = this.FindOrCreateParam(batchParams, name)
                        param.Value <- this.GetArtificialValue<'Arg>()
                }


    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()

    type Converter<'Source, 'Target> = GenericSetters.Converter<unit, FbParameterCollection, 'Source, 'Target>

    type Configurator<'Config> = GenericSetters.Configurator<unit, FbParameterCollection, 'Config>


/// <summary>
/// Provides methods creating various batch parameter builders.
/// </summary>
type BatchParams() = 
    inherit GenericSetters.GenericSetterBuilder<unit, FbParameterCollection>()
           
/// <summary>
/// The batch parameter mapping override.
/// </summary>
type BatchParamOverride<'Arg> = GenericSetters.Override<unit, FbParameterCollection, 'Arg>    


/// <summary>
/// Batch command config.
/// </summary>
type BatchCommandConfig = 
    {
        ParamBuilders   : BatchParamsImpl.IBuilder list
        Compiler        : ICompiler
    }
    with
        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddConverter(convert: 'Source -> 'Target) = 
            { this with 
                ParamBuilders = 
                    BatchParamsImpl.Converter<'Source, 'Target>(convert) :: 
                    this.ParamBuilders 
            }

        /// <summary>
        /// Adds a configurator for parameter builders of types determined by CanBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with 
                ParamBuilders = BatchParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders 
            }


/// <summary>
/// Provides methods creating batch processing functions.
/// </summary>
type BatchCommandBuilder<'DbKey>(dbKey: 'DbKey, ?config: BatchCommandConfig) = 

    let builders = defaultArg (config |> Option.map (fun c -> c.ParamBuilders)) (BatchParamsImpl.getDefaultBuilders())
    let compiler = defaultArg (config |> Option.map (fun c -> c.Compiler)) (LinqExpressionCompiler())

    /// <summary>
    /// Generates a function performing batch processing.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command.
    /// </param>
    /// <param name="specifier">
    /// The parameter specifier.
    /// </param>
    member __.Command(commandText: string, specifier: BatchParamSpecifier<'Record>): 'Record seq -> DbCall<'DbKey, FbBatchNonQueryResult> = 
        let provider = GenericSetters.BaseSetterProvider<unit, FbParameterCollection>(builders, compiler)
        let setter = specifier(provider, ())
        fun (records: 'Record seq) (connector: IConnector<'DbKey>) ->
            async {
                use command = new FbBatchCommand(commandText)
                command.Connection <- connector.GetConnection(dbKey) :?> FbConnection
                command.Transaction <- connector.GetTransaction(dbKey) :?> FbTransaction
                for r in records do
                    let batchParams = command.AddBatchParameters()
                    setter.SetValue(r, None, batchParams)
                let! token = Async.CancellationToken
                return! command.ExecuteNonQueryAsync(token) |> Async.AwaitTask
            }

    /// <summary>
    /// Generates a function performing batch processing.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command.
    /// </param>
    member this.Command<'Record>(commandText: string): 'Record seq -> DbCall<'DbKey, FbBatchNonQueryResult> = 
        this.Command(commandText, BatchParams.Auto<'Record>())


/// <summary>
/// Provides methods creating batch processing functions.
/// </summary>
type BatchCommandBuilder(?config: BatchCommandConfig) = 
    inherit BatchCommandBuilder<unit>((), ?config = config)

