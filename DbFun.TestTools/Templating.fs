namespace DbFun.TestTools

open DbFun.Core

module Templating = 

    let where condition = Templating.expand "WHERE-CLAUSE" " where " " and " condition
    let orderBy defVal (template, parameters) = Templating.expand "ORDER-BY-CLAUSE" " order by " ", " (parameters |> Option.defaultValue defVal |> string) (template, parameters)
    let join spec = Templating.expand "JOIN-CLAUSES" " " " " spec
    let groupBy field = Templating.expand "GROUP-BY-CLAUSE" "group by " ", " field
    let having condition = Templating.expand "HAVING-CLAUSE" "having " " and " condition
