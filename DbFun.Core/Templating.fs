namespace DbFun.Core

module Templating = 

    /// <summary>
    /// Expands some template placeholder with a value.
    /// </summary>
    /// <remarks>
    /// If the expansion occurs for the first time, the clause is added before a value.
    /// Otherwise a value is followed by a separator.
    /// </remarks>
    /// <param name="placeholder">
    /// The placeholder to be replaced with a value.
    /// </param>
    /// <param name="clause">
    /// The clause (e.g. WHERE, ORDER BY, HAVING) to be added when the value is placed for the first time.
    /// </param>
    /// <param name="separator">
    /// The separator injected between subsequent occurrances of a value.
    /// </param>
    /// <param name="template">
    /// The template to be expanded.
    /// </param>
    /// <param name="value">
    /// The value to replace a placeholder.
    /// </param>
    /// <param name="parameters">
    /// The template parameters.
    /// </param>
    let expand (placeholder: string) (clause: string) (separator: string) (value: string) (template: string, parameters: 'Params) : string * 'Params =
        let expanded = 
            if template.Contains("{{" + placeholder + "}}")
            then template.Replace("{{" + placeholder + "}}", clause + "{{" + placeholder + "!}}" + value)
            else template.Replace("{{" + placeholder + "!}}", "{{" + placeholder + "!}}" + value + separator)
        expanded, parameters

    /// <summary>
    /// Removes all remaining placeholders from an expanded template, making it valid sql command.
    /// </summary>
    /// <param name="template">
    /// The template to be cleaned-up.
    /// </param>
    let cleanUp (template: string) = 
        template.Split([| "{{"; "}}" |], System.StringSplitOptions.None) 
        |> Seq.mapi (fun i s -> if i % 2 = 0 then s else "")
        |> String.concat ""

    /// <summary>
    /// Applies a template transformation when the guard condition is met or parameters is not specified (i.e. parameters is None).
    /// </summary>
    /// <param name="guard">
    /// The template transformation condition.
    /// </param>
    /// <param name="expand">
    /// The template transformation function.
    /// </param>
    /// <param name="template">
    /// The template.
    /// </param>
    /// <param name="parameters">
    /// The query parameters object.
    /// </param>
    let applyWhen (guard: 'Params -> bool) (expand: string * 'Params option-> string * 'Params option) (template: string, parameters: 'Params option) = 
        if parameters |> Option.map guard |> Option.defaultValue true then 
            expand(template, parameters) 
        else 
            template, parameters

    /// <summary>
    /// Applies a template transformation with a value extracted from query parameters (or default value if parameters object is None).
    /// </summary>
    /// <param name="getter">
    /// The function extracting a value  from  query parameters.
    /// </param>
    /// <param name="expand">
    /// The template transformation function.
    /// </param>
    /// <param name="template">
    /// The template.
    /// </param>
    /// <param name="parameters">
    /// The query parameters object.
    /// </param>
    let applyWith (getter: 'Params -> 'T) (expand: string * 'T option -> string * 'T option) (template: string, parameters: 'Params option) = 
        expand (template, parameters |> Option.map getter) |> fst, parameters
           
    /// <summary>
    /// Enumarets over items and expands itemTemplate with indexes of subsequent items, concatenates expanded templates
    /// and replaces occurance of a placeholder in a parent template.
    /// </summary>
    /// <param name="placeholder">
    /// The placeholder name of expanded items in a parent template. 
    /// </param>
    /// <param name="itemTemplate">
    /// The item template.
    /// </param>
    /// <param name="separator">
    /// The separator used when concatenating expanded items.
    /// </param>
    /// <param name="template">
    /// The parent template.
    /// </param>
    /// <param name="count">
    /// The number of repetitions.
    /// </param>
    let enumerate placeholder (itemTemplate: string) separator (template: string, count: int option) = 
        let values = 
            if count |> Option.defaultValue 0 > 0 then
                Seq.init (count |> Option.defaultValue 0) (fun i -> itemTemplate.Replace("{{IDX}}", i.ToString())) |> String.concat separator
            else
                itemTemplate.Replace("{{IDX}}", "")
        template.Replace(sprintf "{{%s}}" placeholder, values), count

    /// <summary>
    /// Defines a template transformation using template string and transformation function.
    /// </summary>
    /// <param name="template">
    /// The template.
    /// </param>
    /// <param name="expand">
    /// The template transformation function.
    /// </param>
    /// <param name="parameters">
    /// The query parameters object.
    /// </param>
    let define (template: string) (expand: string * 'Params option -> string * 'Params option) (parameters: 'Params option) = 
        expand(template, parameters) 
        |> fst 
        |> cleanUp
