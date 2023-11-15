namespace DbFun.Core.Builders

open System.Data
open DbFun.Core
open DbFun.Core.Diagnostics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System

/// <summary>
/// The query builder configuration data.
/// </summary>
type QueryConfig = 
    {
        CreateConnection    : unit -> IDbConnection
        ParamBuilders       : ParamsImpl.IBuilder list
        OutParamBuilders    : OutParamsImpl.IBuilder list
        RowBuilders         : RowsImpl.IBuilder list
        Timeout             : int option
        LogCompileTimeErrors: bool
    }
    with 
        /// <summary>
        /// Creates default configuration.
        /// </summary>
        /// <param name="createConnection">
        /// The function creating database connection (with proper connection string, but not open).
        /// </param>
        static member Default(createConnection): QueryConfig = 
            {
                CreateConnection    = createConnection
                ParamBuilders       = ParamsImpl.getDefaultBuilders()
                OutParamBuilders    = OutParamsImpl.getDefaultBuilders()
                RowBuilders         = RowsImpl.getDefaultBuilders()
                Timeout             = None
                LogCompileTimeErrors= false
            }

        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddParamConverter(convert: 'Source -> 'Target) = 
            { this with ParamBuilders = 
                            ParamsImpl.Converter<'Source, 'Target>(convert) :: 
                            ParamsImpl.SeqItemConverter<'Source, 'Target>(convert) :: 
                            this.ParamBuilders 
            }

        /// <summary>
        /// Adds a converter mapping database values to application values.
        /// </summary>
        /// <param name="convert">
        /// Function converting database column values to application values.
        /// </param>
        member this.AddRowConverter(convert: 'Source -> 'Target) = 
            { this with 
                RowBuilders = RowsImpl.Converter<'Source, 'Target>(convert) :: this.RowBuilders
                OutParamBuilders = OutParamsImpl.Converter<'Source, 'Target>(convert) ::  this.OutParamBuilders
            }

        member this.AddParamConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with ParamBuilders = ParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders }

        member this.AddRowConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with 
                RowBuilders = RowsImpl.Configurator<'Config>(getConfig, canBuild) :: this.RowBuilders 
                OutParamBuilders = OutParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.OutParamBuilders
            }

        member this.AddConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            this.AddParamConfigurator(getConfig, canBuild)
                .AddRowConfigurator(getConfig, canBuild)

/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig) =

    let compileTimeErrorLog = if config.LogCompileTimeErrors then Some (ref<CompileTimeErrorLog> []) else None

    let executePrototypeQuery(commandType: CommandType, commandText: string, setParams: IDbCommand -> unit, resultReaderBuilder: IDataReader -> IResultReader<'Result>) =
        use connection = config.CreateConnection()
        connection.Open()
        use command = connection.CreateCommand()
        command.CommandType <- commandType
        command.CommandText <- commandText
        setParams(command)
        use prototype = command.ExecuteReader(CommandBehavior.SchemaOnly)
        resultReaderBuilder(prototype)

    let executeQuery (provider: IConnector, commandText: string, resultReader: IResultReader<'Result>, setParams: IDbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.Text
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            use! dataReader = Executor.executeReaderAsync(command, CommandBehavior.Default)
            return! resultReader.Read(dataReader)
        }

    let executeProcedure (provider: IConnector, commandText: string, outParamGetter: IOutParamGetter<'OutParams>, resultReader: IResultReader<'Result>, setParams: IDbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.StoredProcedure
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            outParamGetter.Create(command)
            use! dataReader = Executor.executeReaderAsync(command, CommandBehavior.Default) 
            let! result = resultReader.Read(dataReader)
            return result, outParamGetter.Get(command)
        }

    let handleException (sourcePath: string, sourceLine: int, ex: exn) = 
        match compileTimeErrorLog with
        | Some errorLog -> 
            errorLog.Value <- (sourceLine, sourcePath, ex) :: errorLog.Value
            fun _ -> 
                raise <| AggregateException("One or more exceptions occured when compiling queries.", 
                            errorLog.Value 
                            |> List.map (fun (line, source, ex) -> CompileTimeException($"Cannot compile query in {sourcePath}, line: {sourceLine}", ex) :> exn))
        | None ->
            raise <| CompileTimeException($"Cannot compile query in {sourcePath}, line: {sourceLine}", ex)

    /// <summary>
    /// Creates query builder object with default configuration
    /// </summary>
    /// <param name="createConnection">
    /// Function creating connection, assigned with a proper connection string, but not open.
    /// </param>
    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))

    /// <summary>
    /// The configuration of the query builder.
    /// </summary>
    member __.Config = config

    /// <summary>
    /// Creates new builder with the specified command timeout.
    /// </summary>
    /// <param name="timeout">
    /// The timeout value in seconds.
    /// </param>
    member __.Timeout(timeout: int) = 
        QueryBuilder({ config with Timeout = Some timeout })

    /// <summary>
    /// Creates new builder with compile-time error logging and deferred exceptions.
    /// </summary>
    member __.LogCompileTimeErrors() = 
        QueryBuilder({ config with LogCompileTimeErrors = true })

    /// <summary>
    /// The list of compile time errors.
    /// </summary>
    member __.CompileTimeErrorLog = 
        match compileTimeErrorLog with
        | Some log -> log.Value
        | None -> []

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="createParamSetter">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    member __.TemplatedSql (createParamSetter: BuildParamSetter<'Params>, 
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : BuildResultReader<'Result> -> ('Params option -> string) -> 'Params -> IConnector -> Async<'Result> =         
        fun (createResultReader: BuildResultReader<'Result>) (template: 'Params option -> string) ->
            try
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter = createParamSetter(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)
                let resultReader = executePrototypeQuery(CommandType.Text, template(None), paramSetter.SetArtificial, createResultReader')

                fun (parameters: 'Params) (provider: IConnector) ->
                    executeQuery(provider, template(Some parameters), resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql (createParamSetter: BuildParamSetter<'Params>, 
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : BuildResultReader<'Result> -> string -> 'Params -> IConnector -> Async<'Result> =         
        fun (createResultReader: BuildResultReader<'Result>) (commandText: string) ->
            this.TemplatedSql(createParamSetter, sourcePath, sourceLine) createResultReader (fun _ -> commandText)

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> IConnector -> Async<'Result> = 
        fun (createResultReader: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   createParamSetter3: BuildParamSetter<'Params3>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> 'Params3 -> IConnector -> Async<'Result> = 
        fun (createResultReader: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)                    

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2, parameters3))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   createParamSetter3: BuildParamSetter<'Params3>,
                   createParamSetter4: BuildParamSetter<'Params4>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> IConnector -> Async<'Result> = 
        fun (createResultReader: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                let paramSetter4 = createParamSetter4(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)                    
                    paramSetter4.SetArtificial(command)                    

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)
                    paramSetter4.SetValue(parameters4, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2, parameters3, parameters4))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with five curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="createParamSetter5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   createParamSetter3: BuildParamSetter<'Params3>,
                   createParamSetter4: BuildParamSetter<'Params4>,
                   createParamSetter5: BuildParamSetter<'Params5>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> IConnector -> Async<'Result> = 
        fun (createResultReader: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                let paramSetter4 = createParamSetter4(provider, ())
                let paramSetter5 = createParamSetter5(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)                    
                    paramSetter4.SetArtificial(command)                    
                    paramSetter5.SetArtificial(command)                    

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4, parameters5: 'Params5) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)
                    paramSetter4.SetValue(parameters4, command)
                    paramSetter5.SetValue(parameters5, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (parameters5: 'Params5) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2, parameters3, parameters4, parameters5))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="createParamSetter">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The stored procedure name.
    /// </param>
    member __.Proc (createParamSetter: BuildParamSetter<'Params>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : BuildOutParamGetter<'OutParams> -> BuildResultReader<'Result> -> string -> 'Params -> IConnector -> Async<'Result * 'OutParams> = 
        fun (createOutParamGetter: BuildOutParamGetter<'OutParams>) (resultReaderBuilder: BuildResultReader<'Result>) (procName: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter = createParamSetter(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = createOutParamGetter(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultReaderBuilder')
                fun (parameters: 'Params) (provider: IConnector) ->
                    executeProcedure(provider, procName, outParamGetter, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The stored procedure name.
    /// </param>
    member __.Proc (createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : BuildOutParamGetter<'OutParams> -> BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> IConnector -> Async<'Result * 'OutParams> = 
        fun (createOutParamGetter: BuildOutParamGetter<'OutParams>) (resultReaderBuilder: BuildResultReader<'Result>) (procName: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = createOutParamGetter(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultReaderBuilder')

                let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command)
                    paramSetter2.SetValue(parameters2, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                    executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with three curried args invoking stored procedure.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The stored procedure name.
    /// </param>
    member __.Proc (createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createParamSetter3: BuildParamSetter<'Params3>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : BuildOutParamGetter<'OutParams> -> BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> 'Params3 -> IConnector -> Async<'Result * 'OutParams> = 
        fun (createOutParamGetter: BuildOutParamGetter<'OutParams>) (resultReaderBuilder: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = createOutParamGetter(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, commandText, setArtificial, resultReaderBuilder')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command)
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (provider: IConnector) ->
                    executeProcedure(provider, commandText, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3))
            with ex ->
                handleException(sourcePath, sourceLine, ex)
                

    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The stored procedure name.
    /// </param>
    member __.Proc (createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createParamSetter3: BuildParamSetter<'Params3>,
                    createParamSetter4: BuildParamSetter<'Params4>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : BuildOutParamGetter<'OutParams> -> BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> IConnector -> Async<'Result * 'OutParams> = 
        fun (createOutParamGetter: BuildOutParamGetter<'OutParams>) (resultReaderBuilder: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                let paramSetter4 = createParamSetter4(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = createOutParamGetter(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)
                    paramSetter4.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, commandText, setArtificial, resultReaderBuilder')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command)
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)
                    paramSetter4.SetValue(parameters4, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (provider: IConnector) ->
                    executeProcedure(provider, commandText, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3, parameters4))
            with ex ->
                handleException(sourcePath, sourceLine, ex)
                
    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="createParamSetter5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The stored procedure name.
    /// </param>
    member __.Proc (createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createParamSetter3: BuildParamSetter<'Params3>,
                    createParamSetter4: BuildParamSetter<'Params4>,
                    createParamSetter5: BuildParamSetter<'Params5>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : BuildOutParamGetter<'OutParams> -> BuildResultReader<'Result> -> string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> IConnector -> Async<'Result * 'OutParams> = 
        fun (createOutParamGetter: BuildOutParamGetter<'OutParams>) (resultReaderBuilder: BuildResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                let paramSetter4 = createParamSetter4(provider, ())
                let paramSetter5 = createParamSetter5(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = createOutParamGetter(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)
                    paramSetter4.SetArtificial(command)
                    paramSetter5.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, commandText, setArtificial, resultReaderBuilder')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4, parameters5: 'Params5) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command)
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)
                    paramSetter4.SetValue(parameters4, command)
                    paramSetter5.SetValue(parameters5, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (parameters5: 'Params5) (provider: IConnector) ->
                    executeProcedure(provider, commandText, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3, parameters4, parameters5))
            with ex ->
                handleException(sourcePath, sourceLine, ex)
                