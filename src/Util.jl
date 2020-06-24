module Util

using Dates
using Infiltrator
using DataFrames

export checkname

"""
    LSTNLEVEL

Höchster zulässiger Abstand beim Namensvergleich
"""
const LSTNLEVEL = 0.3

RGXINP = r" ?\([^\)]*\)"

"""
    checkname(name::AbstractString, nameref::AbstractString)

Vergleiche Namen mithilfe der Levenshtein-Distanz
"""
function checkname(name::Union{Missing, AbstractString},
                   nameref::Union{Missing, AbstractString})::Bool
    if ismissing(name) || ismissing(nameref) || "" in (name, nameref)
        return false
    elseif name == nameref # Performance 
        return true
    else     
        # entferne Klammerausdrücke
        name = replace(name, RGXINP => "")
        nameref = replace(nameref, RGXINP => "")
        
        ld = levenshtein(name, nameref)
        # if ld > 0 && ld < 0.5 && FLOG
        #     global iolog
        #     println(iolog, name, "\t", nameref, "\t", ld)
        # end
        # println(name, "\t", nameref, "\t", ld)
        return ld < LSTNLEVEL
    end    
end

"""
    checknamebyparts(name, nameref)

Prüfe ob Namen übereinstimmen: Zerlege sie in eine Liste von Wörten und bilde die
Schnittmenge.
"""
function checknamepyparts(name, nameref)
   (name == "" || nameref == "") && return false
    nameelems = splitname(name)
    namerefelems = splitname(nameref)
    common = intersect(nameelems, namerefelems)
    return length(common) > 0
end

"""
    splitname(name::AbstractString)

Teile `name` auf und entferne leere Felder und Ordnungszahlen
"""
function splitname(name::AbstractString)
    rgx = r"[^\w]+"

    filtercard(a) = filter(p -> !occursin(r"^[IVX]+$", p), a)
    filterempty(a) = filter(p -> p != "", a)
    split(name, rgx) |> filterempty |> filtercard
end

"""
    levenshtein(u::AbstractString, v::AbstractString)

Berechne die Levenshtein-Distanz zwischen `u` und `v`. Dividiere durch die Länge der
kürzeren Zeichenkette.
"""
function levenshtein(u::AbstractString, v::AbstractString)
    cau = codeunits(u)
    cav = codeunits(v)
    nu = length(cau)
    nv = length(cav)

    cmove = 0.4
    crepl = 1.0

    Dm = fill(1.0, (nu + 1, nv + 1))

    Dm[1, 1] = 0
    Dm[2:nu + 1, 1] = range(cmove, step = cmove, length = nu)
    Dm[1, 2:nv + 1] = range(cmove, step = cmove, length = nv)

    for i in 2:size(Dm, 1), j in 2:size(Dm, 2)
        c = cau[i-1] == cav[j-1] ? 0 : crepl
        # println(i, ", ", j, ", ", c)
        Dm[i, j] = min(Dm[i-1, j-1] + c,
                       Dm[i, j-1] + cmove, # Einfügung
                       Dm[i-1, j] + cmove) # Löschung
    end
    return Dm[end] / min(nu, nv)
    # return Dm[end]
end

"""
    rowselect(df, v, col)

Gib eine View auf `df` zurück, wo die Werte in `col` mit `v` übereinstimmen (Zahl)
oder wo `v` in der Spalte `col` von `df` vorkommt.
"""
function rowselect(df::AbstractDataFrame, v::Number, col::Symbol)
    # Sieht nicht so aus, ist aber vergleichsweise schnell
    ix = v .== df[!, col]
    return @view df[ix, :]
end

function rowselect(df::AbstractDataFrame, v::Union{AbstractString, Regex}, col::Symbol)
    ix = occursin.(v, @view df[!, col])
    return @view df[ix, :]
end

function rowselect(df::AbstractDataFrame, v, dix::Dict{Int, T}) where T
    return @view df[get(dix, v, Int[]), :]
end

"""
    makeindex(df, colname)

Erzeuge einen Index auf `df` ausgehend vom Feld `col`.
"""
function makeindex(df, colname)
    col = df[!, colname]
    coltype = eltype(col)
    di = Dict{coltype, Array{Int, 1}}()
    ciep = unique(col)
    ix = BitArray(undef, nrow(df))
    for iep in ciep
        ix = col .== iep
        di[iep] = findall(ix)
    end
    return di
end

"""
    makedict(path::AbstractString, delim='\t')
"""
function makedict(path::AbstractString, delim='\t')
    dss = Dict{String, String}()
    open(path, "r") do io
        while !eof(io)
            line = readline(io)
            match(r"^[[:space:]]*$", line) != nothing && continue
            p = split(line, "\t")
            dss[p[1]] = p[2]
        end
    end
    dss
end

end
