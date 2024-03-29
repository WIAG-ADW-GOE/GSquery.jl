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
const KEYQDPLACE = :Amtsort
const KEYQDTYPE = :Amtsart
const KEYQDBEGIN = :Amtsbeginn
const KEYQDEND = :Amtsende
const KEYQDIDMONASTERY = :ID_Kloster

# Feldbezeichnungen in der Personendatenbank
const KEYGSTYPE = "bezeichnung"
const KEYGSBEGIN = "von"
const KEYGSEND = "bis"
const KEYGSIDMONASTERY = "klosterid"

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
function evaluate!(record, dfocc::Union{AbstractDataFrame, Nothing}, tolocc, occmcols)
    if isnothing(dfocc) return end
    maxkey = String[]
    for row in eachrow(dfocc)
        key = evaluatesingle(record, row, tolocc, occmcols)
        if islesskey(maxkey, key)
            maxkey = key
        end
    end
    append!(record["amuster"], maxkey)
    return nothing
end

"""
    evaluatesingle!(record, row, tolocc, occmcols)

Bewerte Daten zum Amt. `row` Zeile einer Ämtertabelle.
"""
function evaluatesingle(record, row, tolocc, occmcols)::Vector{String}
    # Für die Bischöfe vor 1198 gibt es oft nur eine Angabe für das
    # Jahrhundert. "[4. Jh.]"

    matchkey = ""
    akey = String[]


    ftype = false
    fbegin = false
    fend = false
    fplace = false
    score = 0

    typeqd = KEYQDTYPE in occmcols ? row[KEYQDTYPE] : missing
    diocqd = KEYQDPLACE in occmcols ? row[KEYQDPLACE] : missing
    idmonasteryqd = KEYQDIDMONASTERY in occmcols ? row[KEYQDIDMONASTERY] : missing

    beginqd::Union{Int, Missing} = missing
    if KEYQDBEGIN in occmcols
        sdate = row[KEYQDBEGIN]
        if Util.hasdata(sdate)
            beginqd = parsedate(row[KEYQDBEGIN])
        end
    end

    endqd::Union{Int, Missing} = missing
    if KEYQDEND in occmcols
        sdate = row[KEYQDEND]
        if Util.hasdata(sdate)
            endqd = parsedate(row[KEYQDEND])
        end
    end

    maxkey = String[]
    for occrec in record["aemter"]
        key = String[]
        # Betrachte nur passende Ämter in `occupations`.
        occgs = occrec[KEYGSTYPE]
        rgm = match(rgxocc, occgs)
        rgm == nothing && continue

        fplace = false
        ftype = false
        fbegin = false
        fend = false

        # Amtsende
        if !ismissing(endqd) && endqd != "" && evaluatedate(occrec[KEYGSEND], endqd, tolocc)
            push!(key, "ae")
        end

        # Amtsbeginn
        if !ismissing(beginqd) && beginqd != "" && evaluatedate(occrec[KEYGSBEGIN], beginqd, tolocc)
            push!(key, "ab")
        end


        # Amtsort or ID_Kloster
        if (!ismissing(diocqd) && diocqd != "" && matchplace(occrec, diocqd)
            || (!ismissing(idmonasteryqd) && string(idmonasteryqd) == occrec[KEYGSIDMONASTERY]))
            push!(key, "ao")
        end

        # Amtsbezeichnung
        ftype = !ismissing(typeqd) && typeqd != "" && matchoccupation(occgs, typeqd)
        if ftype
            push!(key, "at")
        end

        if islesskey(maxkey, key)
            maxkey = key
            record["amt"] = occrec
        end
    end

    return maxkey
end

let
    global parsedate
    rgxyear = r"[0-9]?[0-9]?[0-9]{2}"

    """

    Gib `nothing` zurück, wenn kein gültiges Datum gefunden werden kann
    """
    function parsedate(sdate::Union{AbstractString, Missing})::Union{Int, Missing}
        valdate = missing
        if ismissing(sdate) return valdate end

        rgm = match(rgxyear, sdate)
        if rgm == nothing
            @warn ("Ungültiges Datum in: " * sdate)
        else
            valdate = parse(Int, rgm.match)
        end

        return valdate
    end
end

function parsedate(sdate::Int)::Int
    sdate
end

function evaluatedate(sdategs::Union{AbstractString, Missing},
                      dateqd::Union{Int, Missing},
                      tolocc)
    if ismissing(dateqd) || isnothing(dateqd) return false end
    if ismissing(sdategs) return false end
    if !Util.hasdata(sdategs) return false end
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
        Util.checkname(occqd, occgs) && return true
    end
    return false

end

getlastocc(record) = getlastoccinrecord(record["aemter"])

"""
    getlastoccinrecord(occs)

Gib das Amt mit dem größten Amtsende-Datum zurück
"""
function getlastoccinrecord(occs)
    maxend = 0
    occend = 0
    lastocc::Union{eltype(occs), Nothing} = nothing
    if length(occs) > 0
        lastocc = occs[1]
    end
    for occ in occs
        soccend = occ[KEYGSEND]
        if Util.hasdata(soccend)
            occend = parsedate(soccend)
            if occend > maxend
                maxend = occend
                lastocc = occ
            end
        end
    end
    lastocc
end


end # module Occ
