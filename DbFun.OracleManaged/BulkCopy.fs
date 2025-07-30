namespace DbFun.OracleManaged.Builders

open DbFun.Core
open DbFun.Core.Builders
open System.Data
open System

module BulkCopyParamsImpl = 

    type IParamSetter<'Arg> = GenericSetters.ISetter<DataRow, 'Arg>

    type IParamSetterProvider = GenericSetters.ISetterProvider<DataTable, DataRow>

    type ParamSpecifier<'Arg> = IParamSetterProvider * DataTable -> IParamSetter<'Arg>

    type IBuilder = GenericSetters.IBuilder<DataTable, DataRow>

    type SimpleBuilder() = 

        interface IBuilder with

            member __.CanBuild(argType: System.Type): bool = 
                Types.isSimpleType argType

            member __.Build(name: string, _: IParamSetterProvider, table: DataTable): IParamSetter<'Arg> = 
                let ordinal = ref 0
                { new IParamSetter<'Arg> with
                      member __.SetValue(value: 'Arg, _: int option, row: DataRow): unit = 
                          row.SetField(ordinal.Value, value)
                      member __.SetNull(_: int option, row: DataRow): unit = 
                          row.[ordinal.Value] <- DBNull.Value
                      member __.SetArtificial(_: int option, _: DataRow): unit = 
                          let column = table.Columns.Add(name, typeof<'Arg>) 
                          ordinal.Value <- column.Ordinal
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()


    type Converter<'Source, 'Target> = GenericSetters.Converter<DataTable, DataRow, 'Source, 'Target>

    type Configurator<'Config> = GenericSetters.Configurator<DataTable, DataRow, 'Config>


open BulkCopyParamsImpl
open Oracle.ManagedDataAccess.Client
open DbFun.Core.Builders.Compilers

type BulkCopyParams() = 
    inherit DbFun.Core.Builders.GenericSetters.GenericSetterBuilder<DataTable, DataRow>()

/// <summary>
/// Bulk copy config.
/// </summary>
type BulkCopyConfig = 
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
                    BulkCopyParamsImpl.Converter<'Source, 'Target>(convert) :: 
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
                ParamBuilders = BulkCopyParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders 
            }


/// <summary>
/// Provides methods creating bulk import functions.
/// </summary>
type BulkCopyBuilder<'DbKey>(dbKey: 'DbKey, ?config: BulkCopyConfig) = 

    let builders = defaultArg (config |> Option.map (fun c -> c.ParamBuilders)) (getDefaultBuilders())
    let compiler = defaultArg (config |> Option.map (fun c -> c.Compiler)) (LinqExpressionCompiler())

    /// <summary>
    /// Generates a function performing bulk import.
    /// </summary>
    /// <param name="specifier">
    /// The parameter builder.
    /// </param>
    /// <param name="tableName">
    /// The target table name.
    /// </param>
    member __.WriteToServer<'Record>(specifier: ParamSpecifier<'Record>, ?tableName: string): 'Record seq -> DbCall<'DbKey, unit> = 
        let dataTable = new DataTable()
        let provider = GenericSetters.BaseSetterProvider<DataTable, DataRow>(builders, compiler)
        let setter = specifier(provider, dataTable)
        setter.SetArtificial(None, null)
        fun (records: 'Record seq) (connector: IConnector<'DbKey>) ->
            let dataRow = dataTable.NewRow()
            async {
                let rows = 
                    seq {                                
                        for r in records do
                            setter.SetValue(r, None, dataRow)
                            yield dataRow
                    } |> Seq.toArray
                let bulkCopy = new OracleBulkCopy(connector.GetConnection(dbKey) :?> OracleConnection)
                bulkCopy.DestinationTableName <- defaultArg tableName typeof<'Record>.Name
                bulkCopy.WriteToServer(rows)
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
        this.WriteToServer(BulkCopyParams.Auto<'Record>(?name = name), ?tableName = tableName)

/// <summary>
/// Provides methods creating bulk import functions.
/// </summary>
type BulkCopyBuilder(?config: BulkCopyConfig) = 
    inherit BulkCopyBuilder<unit>((), ?config = config)
