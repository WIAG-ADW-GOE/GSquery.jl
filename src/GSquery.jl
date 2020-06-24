# Ergänze Tabellen mit Personendaten um Angaben aus dem digitalen Personenregister
# des Projektes Germania Sacra
#
# Projektseite
# https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/
#
# Digitales Personenregister
# http://germania-sacra-datenbank.uni-goettingen.de/
#
# API
# http://personendatenbank.germania-sacra.de/api/v1.0/person
# https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/schnittstellen-und-linked-data/
#
# Dokumentation
# ../../README.md
#
"""
    module GSquery

Finde die GS-Nummer und weitere Daten zu Personen im digitalen Personenregister
der Germania Sacra
"""
module GSquery

using DataFrames
using JSON
using HTTP
using Dates
using Infiltrator
using Logging

include("./Util.jl")
include("./GSOcc.jl")

"""
    QRecord

Element im Verzeichnis "records" in einer Anfrage an die Personendatenbank.
"""
struct QRecord
    data::Dict{String, Any}
end

import Base.getindex
getindex(r::QRecord, key::AbstractString) = getindex(r.data, key)

# Parameter

"""
    URLGS

URL des API der Germania Sacra
"""
const URLGS="http://personendatenbank.germania-sacra.de/api/v1.0/person"

"""
    URLGSINDEX

URL des Index API der Germania Sacra
"""
const URLGSINDEX="http://germania-sacra-datenbank.uni-goettingen.de/persons/index"


"""
    minscore

Ein Datensatz mit mindestens diesen Übereinstimmungen wird übernommen.
"""
global minscore = QRecord(Dict("muster" => "vn ae ab ao", "zeitguete" => 0))

"""
    setminmatchkey(matchkey::AbstractString)

Setze den Parameter für den Mindestwert an Übereinstimmung.

`matchkey`: Muster, das noch als Treffer ausgegeben wird.

Beispiel

`"fn vn"`
"""
function setminmatchkey(matchkey::AbstractString)
    global minscore = QRecord(Dict("muster" => matchkey, "zeitguete" => 0))
end


"""
    LIMITN

Zahl der Datensätze, die pro Anfrage gelesen werden sollen.
"""
const LIMITN = 200

"""
    logpath

Wenn der Pfad nicht leer ist, werden hierhin die Log-Mitteilungen geschrieben.
"""
global logpath = ""

"""
    setlogpath(logfile::AbstractString)

Setze den Namen der Datei für Logdaten
""" 
function setlogpath(logfile::AbstractString)
    path = splitdir(logfile)[1]
    path == "" && (path = ".")
    if ispath(path)
        global logpath = logfile
    else
        @error "Pfad nicht gefunden"
    end
end

global filelog = Logging.NullLogger()
global finfiltrate = false

"""
    GSSTRINGCOLS

Liste der Spalten, die aus dem digitalen Personenregister ausgelesen werden.
"""
const GSSTRINGCOLS = [:GSN1_GS,
                      :GSN_GS,
                      :ID_GND_GS,
                      :Qualitaet_GS,
                      :nTreffer_GS,
                      :Vorname_GS,
                      :Vornamenvarianten_GS,
                      :Namenspraefix_GS,
                      :Familienname_GS,
                      :Familiennamenvarianten_GS,
                      :Namenszusatz_GS,
                      :Geburtsdatum_GS,
                      :Sterbedatum_GS,
                      :Amtbischof_GS,
                      :Amtsbeginn_GS,
                      :Amtsende_GS,
                      :Dioezese_GS,
                      :Aemter_GS,
                      :Dioezesen_GS]

const RANKMAX = 199

"""
    Rank

Verwalte Ranklisten.
"""
module Rank

# Parameter
"""
    fileranks

Dateiname der Liste mit Übereinstimmungsmustern. Wenn die entsprechende Datei nicht vorhanden ist, wird eine Rangliste von Mustern mit `makeranklist`erstellt.

Beispiel:
fn sd vn ae ab ao at
...
vn ab ao
...
ab
ao
at

"""
fileranks = "GSQueryranks.txt"

"""
    setfileranks(fileranks::AbstractString)

Setze den Dateinamen für die Liste mit Übereinstimmungsmustern.
"""
setfileranks(file::AbstractString) = (global fileranks = file)

const SLISTKEY = ["fn", "sd", "vn", "ae", "ab", "ao", "at"]
const DXLISTKEY = Dict((k => i) for (i, k) in enumerate(SLISTKEY))
kpless(a, b) = DXLISTKEY[a] < DXLISTKEY[b]

"""
    makeranklist(le = SLISTKEY)

Erzeuge eine Rangliste. Sie wird in der Regel manuell nachjustiert
und in eine Textdatei geschrieben. Siehe `drank`
"""
function makeranklist(le = SLISTKEY)
    rv = [String[]]
    isshorter(a, b) = length(a) < length(b)
    for e in Iterators.reverse(le)
        rvn = copy(rv)
        for q in rv
            push!(rvn, vcat(e, q))
        end
        rv = rvn
    end
    sort!(rv, alg=MergeSort, lt = isshorter)
    (join(q, " ") for q in Iterators.reverse(rv))
end


# löschen?
"""
        makedrank()
    
Erstelle ein Ranking für die Qualität der Treffer. Das Verzeichnis wird für
Vergleiche genutzt.

Normdaten in das Ranking einzubeziehen, erscheint nicht sinnvoll.
"""
function makedrank()
    keys = String[]

    # Fälle ohne GND-Nummer mit Sterbedatum
    # vollständiger Name
    for mkocc in [true, false],
        mdate in ["sd ae ab ao",
                  "sd ae ao",
                  "sd ab ao",
                  "ae ab ao",
                  "sd ao",
                  "ae ao",
                  "ab ao",
                  "sd",
                  "ao"]

        matchkey = "fn vn"
        matchkey *= " " * mdate
        mkocc && (matchkey *= " at")
        push!(keys, matchkey)
    end
    # Ergänze Schlüssel, die sonst noch als Treffer vorkommen.
    # Erster Lauf 2020-01-31, und Lauf mit dem Rest 2020-02-04
    push!(keys, "sd ae ab ao at")
    push!(keys, "ae ab ao at")


    # Fälle ohne GND-Nummer mit Sterbedatum
    # Teil des Names
    for mkocc in [true, false],
        mdate in ["sd ae ab ao",
                  "sd ae ao",
                  "sd ab ao",
                  "ae ab ao",
                  "sd ao",
                  "ae ao",
                  "ab ao",
                  "sd",
                  "ao"],
        mkn in ["fn", "vn"]

        matchkey = mkn
        matchkey *= " " * mdate
        mkocc && (matchkey *= " at")

        push!(keys, matchkey)
    end
    # Ergänze Schlüssel, die sonst noch als Treffer vorkommen.
    # Erster Lauf 2020-01-31, und Lauf mit dem Rest 2020-02-04

    push!(keys, "fn vn at")
    push!(keys, "ae ao at")
    push!(keys, "ab ao at")
    push!(keys, "fn vn")
    push!(keys, "fn")
    push!(keys, "vn")
    push!(keys, "") # kleinstes Element

    # Man kann hier mit dem Code spielen, bis ein Ausgleich zwischen den
    # Bestandteilen gefunden ist. Zuletzt gilt: Je mehr Angaben, desto besser.
    ltf(a, b) = length(a) < length(b)
    sort!(keys, alg = InsertionSort, lt = ltf, rev = true)
    Dict(key => i for (i, key) in enumerate(keys))
end

# const drank = makedrank()

"""
    drank

Verzeichnis von Mustern zu Rängen.
"""
drank = Dict{String, Int}()

function setdrank()
    global drank = Dict(p => rk for (rk, p) in enumerate(makeranklist()))
end


"""
    readdrank(file::AbstractString)

Lies Liste von Übereinstimmungsmustern aus `file`.
"""
function readdrank(file::AbstractString)
    global drank = Dict(p => rk for (rk, p) in enumerate(eachline(file)))
    mxk = maximum(keys(drank))
    # Der letzte Rang entspricht dem leeren Muster
    if drank[mxk] != ""
        drank[mxk + 1] = ""
    end    
end


function islessinset(a, b)
    pa = get(drank, a, 0)
    pb = get(drank, b, 0)
    if pa == 0
        error("Ungültiger Schlüssel: " * a)
    end
    if pb == 0
        error("Ungültiger Schlüssel: " * b)
    end
    
    return pb < pa
end


getrank(key) = drank[key]

isvalidrank(key) = haskey(drank, key)

ranklist() = sort(collect(drank), lt=(a, b) -> a.second < b.second)

end # module Rank

## Globale Variablen

inputcols = [:ID,
             :Praefix,
             :Vorname,
             :Familienname,
             :Sterbedatum,
             :Amtsart,
             :Amtsbeginn,
             :Amtsende,
             :Bistum,
             :GSN_ID,
             :GND_ID,]


"""
    setinputcols(inputcols)

Setze die Namen der Spalten der Eingabetabelle, die in die Ausgabetabelle übernommen werden sollen.
Ohne Argument: Gib die Liste der Spaltennamen der Eingabetabelle aus.
"""
setinputcols(cols) = (global inputcols = Symbol.(cols))
setinputcols() = inputcols


"""
    setcolnameid(id)

Setze den Namen der Spalte, welche die ID enthält.
"""
setcolnameid(id) = (global inputcols[1] = Symbol(id))

colid() = (global inputcols; getindex(inputcols::Array{Symbol, 1}, 1))

## Funktionen

import Base.isless

function isless(a::QRecord, b::QRecord)
    (Rank.islessinset(a.data["muster"],  b.data["muster"])
     || (a.data["muster"] == b.data["muster"]
             && a.data["zeitguete"] < b.data["zeitguete"]))
end


"""
    getGS(url, params::Dict{String, String}; offset = 0, limit = LIMITN, format = "json")
       -> Verzeichnis von Datensätzen; Dict{String, Any}

Stelle eine Anfrage an die Schnittstelle der Germania Sacra. 
`params`: Paare der Form "Feldname" => "Wert"

## Beispiele
getGS(URLGS, Dict("person.gndnummer" => "138018383"))
getGS(URLGS, Dict("person.vorname" => "Christoph", "person.nachname" => "Berg"))
gstGS(URLGSINDEX, Dict("name" => "Georg Anton Rodenstein")
"""
function getGS(url, params; offset = 0, limit = LIMITN, format = "json")
    params["format"] = format
    params["offset"] = string(0)
    params["limit"] = string(limit)
    
    rq = HTTP.request("GET", url, query=params, readtimeout = 30, retries = 5);

    rds = String(rq.body);
    rds4parser = replace(rds, r"\n *" => "");
    return rdt = JSON.parse(rds4parser)
end

"""
    getGSindex(givenname::AbstractString,
               familyname::AbstractString,
               occupation::AbstractString;
               format = "json",
               place = "",
               offset = 0,
               limit = LIMITN)

Frage die Index-Schnittstelle der GS nach Vorname, Name und Amt ab. Das Jahr nehmen wir
nicht auf, da die Zahl der Treffer übersichtlich bleiben wird.
"""
function getGSindex(givenname::AbstractString,
                    familyname::AbstractString;
                    occupation = "",
                    place = "",
                    format = "json",
                    offset = 0,
                    limit = LIMITN)
    # Beispiel
    # name=Berg%20Konrad&amt=bischof&format=json
    csurl = [("http://germania-sacra-datenbank.uni-goettingen.de/persons/index?"
              * "name=")]
    # http://germania-sacra-datenbank.uni-goettingen.de/persons/index?name=rechberg&format=json
    sep = ""
    if givenname != ""
        push!(csurl, encodeurl(givenname))
        sep = "%20"
    end
    familyname != "" && push!(csurl, sep * encodeurl(familyname))
    occupation != "" &&  push!(csurl, "&amt=" * encodeurl(occupation))
    place != "" && push!(csurl, "&ort=" * encodeurl(place))
    push!(csurl, "&format=" * format)
    push!(csurl, "&offset=" * string(offset))
    push!(csurl, "&limit=" * string(limit))
    surl = join(csurl)

    rdt = Dict{String, Any}()
    rq = HTTP.request("GET", surl, readtimeout = 30, retries = 5);
        
    rds = String(rq.body);
    rds4parser = replace(rds, r"\n *" => "");
    rdt = JSON.parse(rds4parser)
    if length(rdt["records"]) == LIMITN
        @info "Limit erreicht" familyname givenname
    end
    return rdt
end



# löschen?
"""
    encodeurl(s)::String

"""
function encodeurl(s)::String
    cs = String[]
    for c in s
        cu = codeunits(string(c))
        for b in cu
            push!(cs, "%")
            push!(cs, bytes2hex([b]))
        end
    end
    cs
    join(cs)
end


"""
    reconcile!(df::AbstractDataFrame,
               dfocc::AbstractDataFrame,
               nmsg = 40,
               toldateofdeath = 5,
               toloccupation = 5)

Frage GS nacheinander nach Name und Amt und dann nach Name und Ort ab.
Verwende `toldateofdeath` als Toleranz für das Sterbedatum und
`toloccupation` als Toleranz für Amtsdaten
Für Testdaten kann eine View auf `df` übergeben werden.
"""
function reconcile!(df::AbstractDataFrame,
                    dfocc::AbstractDataFrame;
                    nmsg = 40,
                    toldateofdeath = 5,
                    toloccupation = 5)

    global filelog
    global logpath

    if length(Rank.drank) == 0 
        if isfile(Rank.fileranks)
            Rank.readdrank(Rank.fileranks)
        else
            @info "Datei nicht gefunden " Rank.fileranks
            @info "Rangliste wird erzeugt."
            Rank.setdrank()
        end
    end


    dfcols = Symbol.(names(df))
    if !(colid() in dfcols)
        error("Die Eingabetabelle enthält keine passende Spalte für die ID (ist: `$(colid())`). "
              * "Siehe `setcolnameid`")
    end

    startid = df[1, colid()]
    minkey = minscore["muster"]
    minrank = Rank.getrank(minkey)

    if logpath != ""
        logio = open(logpath, "a")
        filelog = SimpleLogger(logio)
        with_logger(filelog) do
            @info now() startid
            @info "Level:" minkey minrank
            @info "Gültige Ämter:" GSOcc.occupations
        end
    end

    @info "Level:" minkey minrank
    @info "Gültige Ämter" GSOcc.occupations

    dictquery = Dict("name" => "",
                     "ort" => "",
                     "amt" => "")

    dbest = Dict{Int, Int}()
    irow = 1
    for row in eachrow(df)
        try
            records = QRecord[]
            irow % nmsg == 0 && println("Datensatz: ", irow)
            nbest = 0
            ffound = false

            iep = row[colid()]
            # Ämter für row
            rowocc = Util.rowselect(dfocc, iep, colid())
            
            if row[:Vorname] != ""   # Bischofseintrag
                
                dictquery["name"] = if row[:Familienname] == ""
                    row[:Vorname]
                else
                    row[:Vorname] * " " * row[:Familienname]
                end
                
                familyname = row[:Familienname] # darf auch leer sein
                # Die Abfrage nach Amt müsste mehrfach durchgeführt werden,
                # weil Ämter sich zum Teil entsprechen. Es erscheint einfacher,
                # nur nach dem Ort einzuschränken und die allfällige größere
                # Trefferliste auszuwerten.
                
                # Abfrage nach Name und Bistum
                places = filter(!ismissing, rowocc[!, :Bistum])
                if length(places) == 0
                    places = [""]
                end
                
                for place in places
                    # Die Schnittstelle akzeptiert auch eine Abfrage, wenn
                    # `place` leer ist.
                    dictquery["ort"] = place

                    gsres = getGS(URLGSINDEX, dictquery)
                    append!(records, evaluate!(gsres, row, rowocc, toldateofdeath, toloccupation))
                    @infiltrate
                end
                if length(records) > 0
                    bestrec, posbest = findmax(records)
                    @infiltrate
                    if bestrec >= minscore
                        nbest = writematch!(row, bestrec, records)
                    else
                        with_logger(filelog) do
                            bestkey = bestrec["muster"]
                            @info "abgelehnt" iep  bestkey
                        end
                    end
                end
                dbest[nbest] = get(dbest, nbest, 0) + 1

            else
                @warn "Kein Vorname für: (keine Abfrage) " row[colid()]
            end
            irow += 1
        catch
            @warn "Fehler bei Datensatz: " row[colid()]
            rethrow()
        end
    end

    if logpath != ""
        flush(logio)
        close(logio)
    end
    
    dbest
end



"""
    reconcilebyname!(df::AbstractDataFrame,
                     nmsg = 40,
                     toldateofdeath = 5,
                     toloccupation = 5,
                     limit = 500)

Frage GS nacheinander nach Name ab.
Verwende `toldateofdeath` als Toleranz für das Sterbedatum und
`toloccupation` als Toleranz für Amtsdaten
Für Testdaten kann eine View auf `df` übergeben werden.
"""
function reconcilebyname!(df::AbstractDataFrame;
                          nmsg = 40,
                          toldateofdeath = 5,
                          toloccupation = 5,
                          limit = 500)

    dbest = Dict{Int, Int}()
    irow = 1

    dictquery = Dict("name" => "")
    
    for row in eachrow(df)
        irow % nmsg == 0 && println("Datensatz: ", irow)
        nbest = 0
        if row[:Vorname] != ""   # Bischofseintrag
            if row[:Familienname] == ""
                dictquery["name"] = row[:Vorname]
            else
                dictquery["name"] = row[:Vorname] * " " * row[:Familienname]
            end
            gsres = getGS(URLGSINDEX, dictquery, limit = limit)
            records = evaluate!(gsres, row, toldateofdeath, toloccupation)
            # Es werden nur die Muster in `drank` berücksichtigt!
            nbest = selectmatches(row, records) # mit Ausgabe
            dbest[nbest] = get(dbest, nbest, 0) + 1
        end
        irow += 1
    end

    dbest
end


"""
    reconcilebyGND!(df::AbstractDataFrame;
                    colgnd = :ID_GND,
                    nmsg = 40,
                    toldateofdeath = 6,
                    tolendofoccupation = 11))

Frage GS anhand des Werts in der Spalte `colgnd` ab. 

Vergleichswert im digitalen Personenregister: 
"""
function reconcilebyGND!(df::AbstractDataFrame;
                         colgnd = :ID_GND,
                         nmsg = 40,
                         toldateofdeath = 6, tolendofoccupation = 11)
    irow = 1
    for row in eachrow(df)
        irow % nmsg == 0 && println("Datensatz: ", irow)
        queryGSbyGND!(row, colgnd, toldateofdeath, tolendofoccupation)
        irow += 1
    end
end


function getGNDnumber(sgndnummer)
    rgx = r"[^/]+$"
    rgm = match(rgx, sgndnummer)
    if rgm == nothing
        @warn ("Keine GND-Nummer gefunden in: " * sgndnummer)
        return ""
    else
        return rgm.match
    end
end

"""
    queryGSbyGND!(row, colgnd, toldateofdeath, tolendofoccupation)

Ergänze `row` um Daten aus dem besten Eintrag in der Personendatenbank.
"""
function queryGSbyGND!(row, colgnd, toldateofdeath, tolendofoccupation)
    records = QRecord[]

    gndin = row[colgnd]
    if gndin != ""
        gndnummer = getGNDnumber(gndin)
        gsres = getGS(URLGS, Dict("person.gndnummer" => gndnumber))
        records = evaluate!(gsres, row, toldateofdeath, tolendofoccupation)
    end

    if length(records) > 0
        writerow!(row, records[1])
    end
end

"""
    queryGSNbyGND!(row, colsrc::Symbol, coldst::Symbol)

Frage GSN aufgrund der GND-Nummer ab. Schreibe die GSN in row[coldst]
"""
function queryGSNbyGND!(row, colsrc::Symbol, coldst::Symbol)
    gndnumber = row[colsrc]
    gsn = ""
    if gndnumber != ""
        gsres = getGS(URLGS, Dict("person.gndnummer" => gndnumber))
        ares = gsres["records"]
        nres = length(ares)
        if nres > 0
            @infiltrate
            row[coldst] = gsn = ares[1]["item.gsn"][1]["nummer"]
        elseif nres > 1
            @warn "Mehrere Treffer für" gndnumber
        end
    end
    gsn
end



function selectmatches(row, records)
    nbest = 0
    ffound = false
    length(records) == 0 && return nbest, ffound

    bestrec, posbest = findmax(records)
    if bestrec >= minscore
        writerow!(row, bestrec)
        nbest = count(isequal(bestrec), records)
        if nbest == 1
            ffound = true
        else
            with_logger(filelog) do
                @info "mehrere Treffer" row[colid()] bestrec["muster"]
            end
        end
    end
    return nbest, ffound
end


"""
     evaluate!(gsres::Dict{String, Any}, row, toldod, tolocc)

Bewerte die Datensätze in `gsres` anhand der Werte in `row`
"""
function evaluate!(gsres::Dict{String, Any}, row, rowocc, toldod, tolocc)
    records = QRecord[]
    nvalid = 0
    matchkey = ""
    sreject = String[]

    for record in gsres["records"]
        # println(record["preferredName"], ": mk: ", mk, " scdod: ", scdod)

        # Die Bewertung wird in `record` in den Feldern `muster` und `zeitguete`
        # abgelegt.
        record["amuster"] = String[]
        record["zeitguete"] = 0
        evaluategnfn!(record, row)
        evaluatedod!(record, row, toldod)
        # Wenn die Sterbedaten vorhanden sind, aber außerhalb der Toleranz
        # liegen, lehne den Datensatz ab.
        if record["zeitguete"] == -1
            push!(sreject, join(record["amuster"], " "))
            continue
        end
        GSOcc.evaluate!(record, rowocc, tolocc)

        # Sortiere die Elemente des Musters entsprechend der Reihenfolge in
        # SLISTKEY
        sort!(record["amuster"], lt = Rank.kpless)
        matchkey = record["muster"] = join(record["amuster"], " ")

        if Rank.isvalidrank(matchkey)
            push!(records, QRecord(record))
        else
            matchkey != "" && push!(sreject, matchkey)
        end
    end

    if length(sreject) > 0
        with_logger(filelog) do
            @info "verworfen:" row[colid()] unique(sreject)
        end
    end

    return records
end


"""
    evaluatedod!(record, row, toldod)

Verwende `toldod` für die Bewertung des Wertes in `:Sterbedatum`.
Wenn beide Daten vorhanden sind, aber außerhalb der Toleranz liegen,
schreibe in das Feld "zeitguete" der Wert -1.
"""
function evaluatedod!(record, row, toldod)
    # Für die Bischöfe vor 1198 gibt es oft nur eine Angabe für das
    # Jahrhundert. "[4. Jh.]"

    # rgxjh = r"([1-9][0-9]?)\. Jh" # wird im Moment nicht weiter verfolgt

    function matchdod(sdodqd::Union{<:AbstractString, Missing}, sdodcand)
        ryear = "[0-9]?[0-9]?[0-9]{2}"
        rgxyear = Regex(ryear)
        rgxdobad = Regex("(" * ryear * ")"
                         * " *((-|bis)[^1-9]*"
                         * "(" * ryear * "))?")
        matchkey = ""
        score = 0
        if ismissing(sdodqd) || sdodqd in ("", "(?)", "?")
            return matchkey, score
        end

        rgm = match(rgxyear, sdodqd)
        if rgm == nothing
            @warn "Ungültiges Sterbedatum '$(sdodqd)'"
            return matchkey, score
        end
        dodqd = parse(Int, rgm.match)
        
        sdodcand == "" && return matchkey, score
        rgm = match(rgxyear, sdodcand)
        if rgm == nothing
            @warn ("Kein Datum gefunden in '" * sdodcand * "' für " * string(row[col(id)]))
            return matchkey, score
        end
        dodcand = parse(Int, rgm.match)
        delta = abs(dodcand - dodqd)
        if delta <= toldod
            score = toldod - delta
            matchkey = "sd"
        else
            score = -1
            matchkey = "dsd"
        end
        return matchkey, score
    end

    function matchdod(dodqd::T, sdodcand) where T<:Real
        ryear = "[0-9]?[0-9]?[0-9]{2}"
        rgxyear = Regex(ryear)
        rgxdobad = Regex("(" * ryear * ")"
                         * " *((-|bis)[^1-9]*"
                         * "(" * ryear * "))?")
        matchkey = ""
        score = 0
        
        sdodcand == "" && return matchkey, score
        rgm = match(rgxyear, sdodcand)
        if rgm == nothing
            @warn ("Kein Datum gefunden in '" * sdodcand * "' für " * string(row[col(id)]))
            return matchkey, score
        end
        dodcand = parse(Int, rgm.match)
        delta = abs(dodcand - dodqd)
        if delta <= toldod
            score = toldod - delta
            matchkey = "sd"
        else
            score = -1
            matchkey = "dsd"
        end
        return matchkey, score
    end

    sdodqd = row[:Sterbedatum]
    sdodgs = record["person"]["sterbedatum"]

    matchkey, score = matchdod(sdodqd, sdodgs)

    if matchkey != ""
        push!(record["amuster"], matchkey)
        record["zeitguete"] = score
    end
    return matchkey, score
end



"""

Suche nach Familienname und oder Vorname
"""
function evaluategnfn!(record, row)
    ffn = false
    fgn = false
    # Falls es keinen Familiennamen gibt, wird von `checkkey` zurückgegeben
    familyname = row[:Familienname]
    givenname = row[:Vorname]
    # Die Felder in der GS-Antwort sind immer vorhanden

    matchkey = String[]

    ffn |= Util.checkname(familyname, record["person"]["familienname"])
    fgn |= Util.checkname(givenname, record["person"]["vorname"])
    if ffn && fgn
        matchkey = ["fn", "vn"]
    else
        ffn |= Util.checkname(familyname, record["person"]["familiennamenvarianten"])
        fgn |= Util.checkname(givenname, record["person"]["vornamenvarianten"])
        if ffn && fgn
            matchkey = ["fn", "vn"]
        elseif ffn
            matchkey = ["fn"]
        elseif fgn
            matchkey = ["vn"]
        end
    end

    if matchkey != String[]
        append!(record["amuster"], matchkey)
    end
    return matchkey
end


function makequerygnfn(givenname::AbstractString, familyname::AbstractString)::String
    encgivenname = encodeurl(givenname)
    encfamilyname = encodeurl(familyname)
    return ("query[0][field]=person.vorname&query[0][operator]=contains"
            * "&query[0][value]=" * encgivenname
            * "&query[0][connector]=or"
            * "&query[1][field]=person.vornamenvarianten&query[1][operator]=contains"
            * "&query[1][value]=" * encgivenname
            * "&query[1][connector]=and"
            * "&query[2][field]=person.familienname&query[2][operator]=contains"
            * "&query[2][value]=" * encfamilyname
            * "&query[2][connector]=or"
            * "&query[3][field]=person.familiennamenvarianten&query[3][operator]=contains"
            * "&query[3][value]=" * encfamilyname
            * "&query[3][connector]=and"
            * "&query[4][field]=amt.bezeichnung&query[4][operator]=contains"
            * "&query[4][value]=Bischof"
            * "&format=json"
            * "&limit=" * string(LimitN))
end

function makequerygn(givenname::AbstractString)::String
    encgivenname = encodeurl(givenname)
    return ("query[0][field]=person.vorname&query[0][operator]=contains"
            * "&query[0][value]=" * encgivenname
            * "&query[0][connector]=or"
            * "&query[1][field]=person.vornamenvarianten&query[1][operator]=contains"
            * "&query[1][value]=" * encgivenname
            * "&query[1][connector]=and"
            * "&query[2][field]=amt.bezeichnung&query[4][operator]=contains"
            * "&query[2][value]=Bischof"
            * "&format=json"
            * "&limit=" * string(LimitN))
end


function writematch!(row, bestrec, records)
    if bestrec <= minscore
        @error ("Datensatz soll nicht geschrieben werden: " * string(row[colid()]))
    end
    writerow!(row, bestrec)
    nbest = count(isequal(bestrec), records)
    if nbest > 1
        with_logger(filelog) do
            @info "mehrere Treffer:" row[colid()] bestrec["muster"]
        end
    end
    nbest
end

"""
    writerow!(row, record::QRecord)

`record` ist ein Datensatz aus der Liste "records" einer GS-Abfrage.
"""
function writerow!(row, record::QRecord)
    rdt = record.data
    agsn = String[]
    for item in rdt["item.gsn"]
        push!(agsn, item["nummer"])
    end

    # Finde die erste GSN
    gsn1 = (pdelim = (findfirst(',', agsn[1]))) == nothing ? agsn[1] : agsn[1][1:(pdelim - 1)]

    row[:GSN1_GS] = gsn1
    row[:GSN_GS] = join(agsn, ", ")
    row[:ID_GND_GS] = rdt["person"]["gndnummer"]
    row[:Qualitaet_GS] = rdt["muster"]
    row[:QRang_GS] = Rank.getrank(rdt["muster"])
    row[:nTreffer_GS] = "1" # sonst würde die Funktion nicht so aufgerufen
    row[:Vorname_GS] = rdt["person"]["vorname"]
    row[:Vornamenvarianten_GS] = rdt["person"]["vornamenvarianten"]
    row[:Namenspraefix_GS] = rdt["person"]["namenspraefix"]
    row[:Familienname_GS] = rdt["person"]["familienname"]
    row[:Familiennamenvarianten_GS] = rdt["person"]["familiennamenvarianten"]
    row[:Namenszusatz_GS] = rdt["person"]["namenszusatz"]
    row[:Geburtsdatum_GS] = rdt["person"]["geburtsdatum"]
    row[:Sterbedatum_GS] = rdt["person"]["sterbedatum"]
    if haskey(rdt, "bischofsamt")
        row[:Amtbischof_GS] = rdt["bischofsamt"]["bezeichnung"]
        row[:Amtsbeginn_GS] = rdt["bischofsamt"]["von"]
        row[:Amtsende_GS] = rdt["bischofsamt"]["bis"]
        row[:Dioezese_GS] = rdt["bischofsamt"]["dioezese"]
    end
    row[:Aemter_GS] = getocc(rdt)[1]
    # Ämter gibt es in GS mehrere
end

function getocc(rdt)
    aocc = String[]
    aoccplace = String[]
    if haskey(rdt, "aemter")
        for occrec in rdt["aemter"]
            push!(aocc, occrec["bezeichnung"])
            push!(aoccplace, occrec["dioezese"])
        end
    end
    return join(aocc, ", "), join(filter(!isequal(""), aoccplace), ", ")
end



function makeGSDataFrame(df::AbstractDataFrame)
    dfcols = Symbol.(names(df))
    if !(colid() in dfcols)
        error("Die Eingabetabelle enthält keine passende Spalte für die ID (ist: `$(colid())`). "
              * "Siehe `setcolnameid`")
    end

    global inputcols
    cpcols = intersect(inputcols, dfcols)
    dfgs = copy(df[!, cpcols])
    @infiltrate
    # `insertcols!` scheint nicht einfacher
    for col in GSSTRINGCOLS
        dfgs[!, col] .= ""
    end
    dfgs[!, :QRang_GS] .= RANKMAX
    dfgs
end

"""
    clearbylevel!(df::AbstractDataFrame, matchkey::AbstractString, level::Int)

Lösche Datensätze, deren Qualität unter `matchkey`, bzw. `rank` liegt. Die Angaben
sind zur Sicherheit redundant.
"""
function clearbylevel!(df::AbstractDataFrame, matchkey::AbstractString, rank::Int)
    if rank != getrank(matchkey)
        error("Muster und Rang stimmen nicht überein")
    end
    colrank = df[!, :QRang_GS]
    ixr = colrank .> rank
    df[ixr, GSSTRINGCOLS] .= "" # zulässig!
    df[ixr, :QRang_GS] .= RANKMAX
    count(df[!, :QRang_GS] .< RANKMAX)
end

"""
    infolevel!(df::AbstractDataFrame, matchkey::AbstractString)

"""
function infolevel(df::AbstractDataFrame, matchkey::AbstractString)
    rank = getrank(matchkey)
    println("Das Muster '", matchkey, "' hat die Position: ", rank, ".")
    println("Datensätze insgesamt: ", size(df, 1))
    colrank = df[!, :QRang_GS]
    println("Rang größer als '", matchkey, "': ", count(colrank .> rank))
    rankmax = maximum(colrank)
    nvalid = count(colrank .< rankmax)
    ndel = count((colrank .< rankmax) .& (colrank .> rank))
    println("Davon kleiner maximaler Rang : ", ndel)
    println("Es bleiben erhalten: ", nvalid - ndel)
end


"""

Lege ein Verzeichnis von `colsrc zu `coldst` an.
"""
function iddictionary(df::AbstractDataFrame, colsrc::Symbol, coldst::Symbol)
    tsrc = eltype(df[!, colsrc])
    tdst = eltype(df[!, coldst])

    odict = Dict{tsrc, Set{tdst}}()
    for row in eachrow(df)
        vsrc, vdst = row[[colsrc, coldst]]
        if vsrc != "" && vdst != ""
            odict[vsrc] = push!(get(odict, vsrc, Set{tdst}()), vdst)
        end
    end
    odict
end

function compareyear(df::AbstractDataFrame, colA, colB, coldst)
    rgx = r"[0-9]?[0-9]?[0-9][0-9]"
    df[!, coldst] .= ""
    for row in eachrow(df)
        row[:nTreffer_GS] == "" && continue
        a = row[colA]
        b = row[colB]
        a == b == "" && continue
        (a == "" || b == "") && (row[coldst] = "v"; continue)
        rgma = match(rgx, a)
        rgmb = match(rgx, b)
        (rgma == nothing || rgmb == nothing) && (row[coldst] = "?"; continue)
        row[coldst] = string(abs(parse(Int, rgma.match) - parse(Int, rgmb.match)))
    end
end

function comparename(df::AbstractDataFrame, colA, colB, coldst)
    df[!, coldst] .= ""
    for row in eachrow(df)
        row[:nTreffer_GS] == "" && continue
        a = row[colA]
        b = row[colB]
        a == b == "" && continue
        (a == "" || b == "") && (row[coldst] = "v"; continue)
        seta = split(a)
        setb = split(b)
        ni = length(intersect(seta, setb))
        nu = length(union(seta, setb))
        row[coldst] = string(round(Int, (nu - ni)/nu * 100))
    end
end

"""
    deltaID(ref, gs)

Vergleiche IDs: `ref` ist z.B. manuell bestätigt.
"""
function deltaID(ref, gs)
    if gs == ""
        ref == "" ? (return "vv") : (return "xv")
    else
        ref == gs ? (return "t") : (return "f")
    end
end

# löschen?
"""
    getGSindexocc(locc)

Frage GS nach allen Ämtern in `locc` ab. (Alle Bischöfe in der GS).
Beispiel
locc = ["Bischof", "Elekt", "Administrator", "Patriarch", "Metropolit", "Generalvikar"]
"""
function getGSindexocc(locc)
    cols = setdiff(GSSTRINGCOLS, [:Qualitaet_GS, :nTreffer_GS])
    df = DataFrame(fill(String, length(cols)), cols, 0)
    step = 200

    # 

    for occ in locc
        offset = 0
        nocc = 0
        while true
            rqr = getGSindex("", "", occupation = occ, offset = offset, limit = step, fwarn = false)
            offset += step
            lrqr = length(rqr["records"])
            nocc += lrqr
            for rdt in rqr["records"]
                push!(df, readgsrecord(rdt))
            end
            lrqr < 1 && break
        end
        println(occ, ": ", nocc)
    end
    
    return df
end

# löschen?
"""
    readgsrecord(rdt)

Lies daten aus `rdt` in ein Verzeichnis(Dict).
"""
function readgsrecord(rdt)
    agsn = String[]
    @infiltrate typeof(rdt) != Dict{String, Any}
    items = rdt["item.gsn"]
    for item in items
        push!(agsn, item["nummer"])
    end

    # Finde die erste GSN
    gsn1 = (pdelim = (findfirst(',', agsn[1]))) == nothing ? agsn[1] : agsn[1][1:(pdelim - 1)]

    row = Dict{Symbol, String}()
    row[:GSN1_GS] = gsn1
    row[:GSN_GS] = join(agsn, ", ")
    row[:ID_GND_GS] = rdt["person"]["gndnummer"]
    row[:Vorname_GS] = rdt["person"]["vorname"]
    row[:Vornamenvarianten_GS] = rdt["person"]["vornamenvarianten"]
    row[:Namenspraefix_GS] = rdt["person"]["namenspraefix"]
    row[:Familienname_GS] = rdt["person"]["familienname"]
    row[:Familiennamenvarianten_GS] = rdt["person"]["familiennamenvarianten"]
    row[:Namenszusatz_GS] = rdt["person"]["namenszusatz"]
    row[:Geburtsdatum_GS] = rdt["person"]["geburtsdatum"]
    row[:Sterbedatum_GS] = rdt["person"]["sterbedatum"]
    if haskey(rdt, "bischofsamt")
        row[:Amtbischof_GS] = rdt["bischofsamt"]["bezeichnung"]
        row[:Amtsbeginn_GS] = rdt["bischofsamt"]["von"]
        row[:Amtsende_GS] = rdt["bischofsamt"]["bis"]
        row[:Dioezese_GS] = rdt["bischofsamt"]["dioezese"]
    else
        row[:Amtbischof_GS] = ""
        row[:Amtsbeginn_GS] = ""
        row[:Amtsende_GS] = ""
        row[:Dioezese_GS] = ""
    end
    row[:Aemter_GS], row[:Dioezesen_GS] = getocc(rdt)
    # Ämter gibt es in GS mehrere
    return row
end


end # GSquery
