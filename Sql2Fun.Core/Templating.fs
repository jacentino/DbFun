namespace Sql2Fun.Core

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
    let expand (placeholder: string) (clause: string) (separator: string) (value: string) (template: string) : string =
        if template.Contains("{{" + placeholder + "}}")
        then template.Replace("{{" + placeholder + "}}", clause + "{{" + placeholder + "!}}" + value)
        else template.Replace("{{" + placeholder + "!}}", "{{" + placeholder + "!}}" + value + separator)

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


    let applyWhen (guard: 'Params -> bool) (expand: string -> string) (template: string, parameters: 'Params option) = 
        let expanded = 
            match parameters with
            | Some p -> if guard(p) then expand(template) else template
            | None -> expand(template)
        expanded, parameters

    let applyWith (getter: 'Params -> 'T) (defVal: 'T) (expand: 'T -> string -> string) (template: string, parameters: 'Params option) = 
        let expanded = 
            match parameters with
            | Some p -> expand (getter(p)) template
            | None -> expand defVal template
        expanded, parameters

    let define (template: string) (expand: string * 'Params option -> string * 'Params option) (parameters: 'Params option) = 
        expand(template, parameters) 
        |> fst 
        |> cleanUp