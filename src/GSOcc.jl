module GSOcc

using DataFrames
using Infiltrator

import ..Util

# Parameter
"""
    occupations

Liste von Ämtern, die in Betracht gezogen werden.

Beispiel

["Bischof",
 "Vikar",
 "Elekt",
 "Administrator",
 "Patriarch",
 "Metropolit"]
"""
global occupations = ["Bischof", "Vikar", "Elekt", "Administrator", "Patriarch", "Metropolit"]

function regexor(lms::Array{<:AbstractString, 1})
    if length(lms) > 1
        Regex(join(lms, "|"), "i")
    else
        r".+"
    end
end

global rgxocc = regexor(occupations)

"""
    setoccupations(occ::Vector{<:AbstractString})

Setze die Liste der Ämter, die bei der Abfrage in Betracht gezogen werden.
Wenn die Liste leer ist, werden alle Ämter in Betracht gezogen.

`setoccupations()`: Gib die aktuelle Liste aus.

Beispiel

```julia
setoccupations(["Pfarrer", "Vikar"])
```
"""
function setoccupations(occ::Vector{<:AbstractString})
    global occupations = occ
    global rgxocc = regexor(occupations)
    occupations
end

setoccupations() = occupations

"""
    equivrls

Liste von Zuordnungen für als gleichwertig betrachtete Ämter
"""
equivrls = ["Gewählter Bischof" => "Elekt",
            "Erwählter Bischof" => "Elekt",
            "Fürstbischof" => "Bischof"]

"""
    setequivalentoccupations(lequivocc)

Setze Liste von Zuordnungen für als gleichwertig betrachtete Ämter.

`setequivalentoccupations()`: Gib die aktuelle Liste aus.

Beispiel

```julia
setequivalentoccupations(["Gewählter Bischof" => "Elekt",
                          "Erwählter Bischof" => "Elekt"])
```
"""
function setequivalentoccupations(lequivocc)
    global equivrls
    equivrls = [Pair(p...) for p in lequivocc]
end

setequivalentoccupations() = equivrls

const KEYS = [["ae", "ab", "ao", "at"],
              ["ae", "ab", "ao"],
              ["ae", "ao", "at"],
              ["ab", "ao", "at"],
              ["ae", "ab", "at"],
              ["ae", "ao"],
              ["ab", "ao"],
              ["ao", "at"],
              ["ae", "ab"],
              ["ae", "at"],
              ["ab", "at"],
              ["ao"],
              ["ae"],
              ["ab"],
              ["at"],
              []]

# Feldbezeichnungen in den Abfragedaten
const KEYDIOCQD = :Amtsort
const KEYTYPEQD = :Amtsart
const KEYBEGINQD = :Amtsbeginn
const KEYENDQD = :Amtsende

# Feldbezeichnungen in der Personendatenbank
const KEYTYPEGS = "bezeichnung"
const KEYBEGINGS = "von"
const KEYENDGS = "bis"


# Funktionen
function islesskey(a, b)
    findfirst(isequal(a), KEYS) > findfirst(isequal(b), KEYS)
end

"""
    evaluate!(record, dfocc::AbstractDataFrame, tolocc, occmcols)

Bewerte Daten zu Ämtern.

`dfocc`: Ämter in Abfragedaten für diese eine Person.
`tolocc`: Maximal zulässige Abweichung in Start- und Enddatum eines Amtes.
`occmcols`: Spalten in den Abfragedaten
"""
function evaluate!(record, dfocc::AbstractDataFrame, tolocc, occmcols)
    maxkey = String[]
    for row in eachrow(dfocc)
        key = evaluatesingle(record, row, tolocc, occmcols)
        if !islesskey(key, maxkey)
            maxkey = key
        end
    end
    append!(record["amuster"], maxkey)
end

"""
    evaluateooc!(record, row, tolocc)

Bewerte Daten zum Amt. `row` Zeile einer Ämtertabelle.
"""
function evaluatesingle(record, row, tolocc, occmcols)
    # Für die Bischöfe vor 1198 gibt es oft nur eine Angabe für das
    # Jahrhundert. "[4. Jh.]"

    matchkey = ""
    akey = String[]

    # ryear = "([0-9]?[0-9]?[0-9]{2})(/[0-9]?[0-9]\\??)?"
    ryear = "[0-9]?[0-9]?[0-9]{2}"
    rgxyear = Regex(ryear)

    ftype = false
    fbegin = false
    fend = false
    fplace = false
    score = 0

    typeqd = KEYTYPEQD in occmcols ? row[KEYTYPEQD] : ""
    diocqd = KEYDIOCQD in occmcols ? row[KEYDIOCQD] : ""
    beginqd = KEYBEGINQD in occmcols ? parsedate(row[KEYBEGINQD]) : ""
    endqd = KEYENDQD in occmcols ? parsedate(row[KEYENDQD]) : ""

    # Wir bilden nicht das Maximum über die Ämter, jedes passende Amt kann zählen
    for occrec in record["aemter"]
        # Betrachte nur passende Ämter in `occupations`.
        occgs = occrec[KEYTYPEGS]
        rgm = match(rgxocc, occgs)
        rgm == nothing && continue

        fplace = false
        ftype = false
        fbegin = false
        fend = false

        # Amtsort
        fplace = diocqd != "" && matchplace(occrec, diocqd)

        # Amtsbeginn
        fbegin = beginqd != "" && evaluatedate(occrec[KEYBEGINGS], beginqd, tolocc)

        # Amtsende
        fend = endqd != "" && evaluatedate(occrec[KEYENDGS], endqd, tolocc)

        # Amtsbezeichnung
        ftype = typeqd != "" && matchoccupation(occgs, typeqd)

        if ftype || length(occupations) > 0
            # Amt passt oder ist gefiltert
            record["amt"] = occrec
        end

    end

    # Das Amtsende wird höher eingestuft als der Beginn
    fend && push!(akey, "ae")
    fbegin && push!(akey, "ab")
    fplace && push!(akey, "ao")
    ftype && push!(akey, "at")

    return akey
end

"""

Gib `missing` zurück, wenn kein gültiges Datum gefunden werden kann
"""
function parsedate(sdate::Union{AbstractString, Missing})
    rgxyear = r"[0-9]?[0-9]?[0-9]{2}"
    valdate = missing

    ismissing(sdate) && return valdate

    sdate in ("", "(?)", "?") && return valdate

    rgm = match(rgxyear, sdate)
    if rgm == nothing
        @warn ("Ungültiges Datum in: " * sdate)
    else
        valdate = parse(Int, rgm.match)
    end

    return valdate
end

function parsedate(sdate::Int)
    sdate
end

function evaluatedate(sdategs::Union{AbstractString, Missing},
                      dateqd::Union{Int, Missing}, tolocc)
    if ismissing(dateqd) return false end
    if ismissing(sdategs) return false end
    if sdategs == "" return false end
    dategs = parsedate(sdategs)
    if ismissing(dategs) return false end

    delta = abs(dategs - dateqd)
    if delta <= tolocc
        return true
    else
        return false
    end
end


"""
    matchplace(occrec, diocqd::Union{AbstractString, Missing})

Prüfe, ob die Orte in `occrec` zu der Angabe der neuen Quelle passen.
"""
function matchplace(occrec, diocqd::Union{AbstractString, Missing})
    placegs = occrec["ort"]
    diocgs = occrec["dioezese"]
    return (Util.checkname(diocqd, placegs)
            || Util.checkname(diocqd, diocgs))
end

"""
    gettypeoccupation(socc)

Extrahiere die Art des Amtes (= eines aus `occupations`)
"""
function gettypeoccupation(socc::AbstractString)
    global rgxocc
    rgm = match(rgxocc, socc)
    if rgm == nothing
        return ""
    else
        return rgm.match
    end
end

"""
    gettypeoccupation(dfocc)

Extrahiere die Art des Amtes (= eines aus `occupations`)
"""
function gettypeoccupation(dfocc::AbstractDataFrame)
    global rgxocc
    # Beginne mit dem letzen Amt
    aocc = dfocc[!, :Amtsart]
    if length(aocc) == 0
        return ""
    end
    typeocc = ""
    for occ in Iterators.reverse(aocc)
        rgm = match(rgxocc, occ)
        if rgm != nothing
            typeocc = rgm.match
            break
        end
    end
    if typeocc == ""
        typeocc = aocc[end]
    end
    typeocc
end


"""
    matchoccupation(occqd::AbstractString, occgs::AbstractString)

Vergleich Ämter direkt, oder über eine Bezeichnung für ein Bischofsamt.
"""
function matchoccupation(occgs::AbstractString, occqd::AbstractString)

    # Direkte Übereinstimmung
    Util.checkname(occqd, occgs) && return true

    global equivrls
    for rl in equivrls::Array{Pair{String, String}}
        a = replace(occgs, rl)
        b = replace(occqd, rl)
        @infiltrate a == b
        Util.checkname(occqd, occgs) && return true
    end
    return false

end

end # module Occ
