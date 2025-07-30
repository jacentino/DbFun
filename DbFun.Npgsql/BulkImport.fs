namespace DbFun.Npgsql.Builders

open System
open Npgsql
open DbFun.Core
open DbFun.Core.Builders

module BulkImportParamsImpl = 

    type IParamSetter<'Arg> = GenericSetters.ISetter<NpgsqlBinaryImporter, 'Arg>

    type IParamSetterProvider = GenericSetters.ISetterProvider<string list ref, NpgsqlBinaryImporter>

    type ParamSpecifier<'Arg> = IParamSetterProvider * string list ref -> IParamSetter<'Arg>

    type IBuilder = GenericSetters.IBuilder<string list ref, NpgsqlBinaryImporter>

    type SimpleBuilder() = 

        interface IBuilder with

            member __.CanBuild(argType: System.Type): bool = 
                Types.isSimpleType argType

            member this.Build(name: string, _: IParamSetterProvider, names: string list ref): IParamSetter<'Arg> = 
                { new IParamSetter<'Arg> with
                      member __.SetValue(value: 'Arg, _: int option, importer: NpgsqlBinaryImporter): unit = 
                          importer.Write(value)
                      member __.SetNull(_: int option, importer: NpgsqlBinaryImporter): unit = 
                          importer.WriteNull()
                      member __.SetArtificial(_: int option, _: NpgsqlBinaryImporter): unit = 
                          names.Value <- name :: names.Value
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()


    type Converter<'Source, 'Target> = GenericSetters.Converter<string list ref, NpgsqlBinaryImporter, 'Source, 'Target>

    type Configurator<'Config> = GenericSetters.Configurator<string list ref, NpgsqlBinaryImporter, 'Config>


open BulkImportParamsImpl
open DbFun.Core.Builders.Compilers

type BulkImportParams() = 
    inherit DbFun.Core.Builders.GenericSetters.GenericSetterBuilder<string list ref, NpgsqlBinaryImporter>()

/// <summary>
/// Bulk import config.
/// </summary>
type BulkImportConfig = 
    {
        ParamBuilders   : IBuilder list
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
                    BulkImportParamsImpl.Converter<'Source, 'Target>(convert) :: 
                    this.ParamBuilders 
            }

        /// <summary>
        /// Adds a configurator for parameter builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with 
                ParamBuilders = BulkImportParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders 
            }

/// <summary>
/// Provides methods creating bulk import functions.
/// </summary>
type BulkImportBuilder<'DbKey>(dbKey: 'DbKey, ?config: BulkImportConfig) = 

    let builders = defaultArg (config |> Option.map (fun c -> c.ParamBuilders)) (getDefaultBuilders())
    let compiler = defaultArg (config |> Option.map (fun c -> c.Compiler)) (LinqExpressionCompiler())

    /// <summary>
    /// Generates a function performing bulk import.
    /// </summary>
    /// <param name="tableName">
    /// The target table name.
    /// </param>
    /// <param name="setterBuilder">
    /// The parameter builder.
    /// </param>
    member __.WriteToServer<'Record>(specifier: ParamSpecifier<'Record>, ?tableName: string): 'Record seq -> DbCall<'DbKey, unit> = 
        let fieldNames = ref List.empty<string>
        let provider = GenericSetters.BaseSetterProvider<string list ref, NpgsqlBinaryImporter>(builders, compiler)
        let setter = specifier(provider, fieldNames)
        setter.SetArtificial(None, null)
        let copyCommand = 
            sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" 
                (defaultArg tableName (typeof<'Record>.Name.ToLower()))
                (fieldNames.Value |> List.rev |> String.concat ", ")
        fun (records: 'Record seq) (connector: IConnector<'DbKey>) ->
            let npgcon = connector.GetConnection(dbKey) :?> NpgsqlConnection
            async {
                let! token = Async.CancellationToken
                use importer = npgcon.BeginBinaryImport(copyCommand)
                for r in records do
                    do! importer.StartRowAsync(token) |> Async.AwaitTask
                    setter.SetValue(r, None, importer)
                do! importer.CompleteAsync(token).AsTask() |> Async.AwaitTask |> Async.Ignore
            }
            
    /// <summary>
    /// Generates a function performing bulk import.
    /// </summary>
    /// <param name="tableName">
    /// The target table name.
    /// </param>
    /// <param name="name">
    /// The builder name argument.
    /// </param>
    member this.WriteToServer<'Record>(?name: string, ?tableName: string): 'Record seq -> DbCall<'DbKey, unit> = 
        this.WriteToServer(BulkImportParams.Auto<'Record>(?name = name), ?tableName = tableName)


/// <summary>
/// Provides methods creating bulk import functions.
/// </summary>
type BulkImportBuilder(?config: BulkImportConfig) = 
    inherit BulkImportBuilder<unit>((), ?config = config)
