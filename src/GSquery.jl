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
# ../README.md
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

Treffer mit einem schlechterem Muster als `matchkey` werden nicht berücksichtigt.
`setminmatchkey()` gibt das aktuelle Muster aus.

Beispiel

`"fn vn"`: Mindestens Familienname und Vorname müssen übereinstimmen.
"""
function setminmatchkey(matchkey::AbstractString)
    global minscore = QRecord(Dict("muster" => matchkey, "zeitguete" => 0))
end

setminmatchkey() = minscore

"""
    LIMITN

Zahl der Datensätze, die pro Anfrage gelesen werden sollen.
"""
const LIMITN = 300

"""
    logpath

Wenn der Pfad nicht leer ist, werden hierhin die Log-Mitteilungen geschrieben.
"""
global logpath = ""

"""
    setlogpath(logfile::AbstractString)

Setze den Namen der Datei für Logdaten

Wenn der Pfad der Logdatei leer ist (""), werden keine Logdaten geschrieben.
`setlogpath()`: gib den aktuellen Pfad der Logdatei aus.
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

setlogpath() = logpath

global filelog = Logging.NullLogger()

"""
    GSSTRINGCOLS

Liste der Spalten, die aus dem digitalen Personenregister ausgelesen werden.
"""
const GSSTRINGCOLS = [:GSN1_GS,
                      :GSN_GS,
                      :ID_GND_GS,
                      :Qualitaet_GS,
                      :Vorname_GS,
                      :Vornamenvarianten_GS,
                      :Namenspraefix_GS,
                      :Familienname_GS,
                      :Familiennamenvarianten_GS,
                      :Namenszusatz_GS,
                      :Geburtsdatum_GS,
                      :Sterbedatum_GS,
                      :Amt_GS,
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

Dateiname der Liste mit Übereinstimmungsmustern. Wenn die entsprechende Datei nicht vorhanden ist, wird automatisch eine Rangliste von Mustern mit `makeranklist`erstellt.

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
    setfileofranks(fileranks::AbstractString)

Setze den Dateinamen für die Liste mit Übereinstimmungsmustern.

Wenn die entsprechende Datei nicht vorhanden ist, wird für die Abfrage
automatisch eine Rangliste von Mustern erstellt.

`setfileofranks()`: Gib den aktuellen Dateinamen aus.
"""
setfileofranks(file::AbstractString) = (global fileranks = file)
setfileofranks() = return fileranks


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



"""
    drank

Verzeichnis von Mustern zu Rängen.
"""
drank = Dict{String, Int}()

"""
    setdrank(fileranks)

Lies eine Liste mit Übereinstimmungsmustern aus `fileranks`.

`setdrank()` erzeugt eine Standardversion der Liste von Übereinstimmungsmustern.
"""
function setdrank(fileranks)
    global drank
    if isfile(fileranks)
        @info "Lies Übereinstimmungsmuster aus " fileranks
        drank = Rank.readdrank(fileranks)
    else
        @info "Datei nicht gefunden " fileranks
        @info "Liste der Übereinstimmungsmuster wird erzeugt."
        setdrank()
    end
    return nothing
end

function setdrank()
    global drank
    drank = Dict(p => rk for (rk, p) in enumerate(makeranklist()))
end

"""
    readdrank(file::AbstractString)

Lies Liste von Übereinstimmungsmustern aus `file`.
"""
function readdrank(file::AbstractString)
    local drank = Dict(p => rk for (rk, p) in enumerate(eachline(file)))
    # Der letzte Rang entspricht dem leeren Muster
    if !haskey(drank, "")
        drank[""] = maximum(values(drank)) + 1
    end
    drank
end

"""
    islessinset(a, b; drank=drank)

Rückgabewert `drank[b] < drank[a]`

Prüfe, ob `a` und `b` als Schlüssel vorhanden sind. Beachte: Je kleiner der Rang, desto besser ist das Muster.
"""
function islessinset(a, b; drank=drank)
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

"""
    ranklist()

Gib die Liste mit Übereinstimmungsmustern aus.
"""
ranklist() = sort(collect(drank), lt=(a, b) -> a.second < b.second)

end # module Rank

# globale Variablen

inputcols = [:ID,
             :Praefix,
             :Vorname,
             :Familienname,
             :Familiennamenvarianten,
             :Sterbedatum,
             :Amtsart,
             :Amtsbeginn,
             :Amtsende,
             :Amtsort,
             :GSN_ID,
             :GND_ID,]


"""
    setinputcols(cols)

Setze die Namen der Spalten der Eingabetabelle, die in die Ausgabetabelle übernommen werden sollen

`setinputcols()`: Gib die Spaltennamen aus.
"""
function setinputcols(cols)
    global inputcols
    inputcols = Symbol.(cols)
end

setinputcols() = inputcols

"""
    setcolnameid(id)

Setze den Namen der Spalte, welche die ID enthält

`setcolnameid()`: Gib den aktuellen Wert aus

Voreinstellung: `:ID`
"""
setcolnameid(id) = (global inputcols[1] = Symbol(id))
setcolnameid() = inputcols[1]

colid() = (global inputcols; getindex(inputcols::Array{Symbol, 1}, 1))

# Spalten, die vom Programm bewertet werden können.
const MATCHCOLS = [:Vorname, :Vornamenvarianten, :Familienname, :Familiennamenvarianten, :Sterbedatum]
const MATCHOCCCOLS = [:Amtsart, :Amtsbeginn, :Amtsende, :Amtsort]

# Funktionen
import Base.isless

function isless(a::QRecord, b::QRecord)
    (Rank.islessinset(a.data["muster"],  b.data["muster"])
     || (a.data["muster"] == b.data["muster"]
         && a.data["zeitguete"] < b.data["zeitguete"]))
end

import Base.isequal

function isequal(a::QRecord, b::QRecord)
    (isequal(a.data["muster"],  b.data["muster"])
     && isequal(a.data["zeitguete"], b.data["zeitguete"]))
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
    qparams = copy(params)
    qparams["format"] = format
    qparams["offset"] = string(0)
    qparams["limit"] = string(limit)

    rq = HTTP.request("GET", url, query=qparams, readtimeout = 30, retries = 5);

    rds = String(rq.body);
    rds4parser = replace(rds, r"\n *" => "");
    return rdt = JSON.parse(rds4parser)
end

"""
    reconcile!(df,
               querycols,
               dfocc;
               nmsg = 40,
               toldateofdeath = 2,
               toloccupation = 2)

Frage das digitale Personenregister nach den Spalten in `querycols` ab.
Vergleiche die gefundenen Datensätze mit Name und Amtsdaten aus dem Abfragedatensatz.
Ergänze `df` für jeden Datensatz mit den Daten aus dem besten Treffer.
Gib nach einer Zahl von `nmsg` Datensätzen eine Fortschrittsmeldung aus.
Verwende `toldateofdeath` als Toleranz für das Sterbedatum und
`toloccupation` als Toleranz für Amtsdaten.

    reconcile!(df::AbstractDataFrame, fields; nmsg = 40)

Frage das digitale Personenregister nach den Feldern in `fields` ab.
Ergänze `df` für jeden Datensatz mit den Daten aus dem erstbesten Treffer.
Gib nach einer Zahl von `nmsg` Datensätzen eine Fortschrittsmeldung aus.

`fields` ordnet jeder Spalte in `df` einen `Feldnamen` im Personenregister zu.

``` julia
fields = Dict(:Vorname => "person.vorname", :Familienname => "person.nachname")
fields = ((:ID_GND => "person.gndnummer"),)
```
"""
function reconcile!(df,
                    querycols,
                    dfocc;
                    nmsg = 40,
                    toldateofdeath = 2,
                    toloccupation = 2)

    global filelog
    global logpath

    if length(Rank.drank) == 0
        Rank.setdrank()
    end

    dfcols = Symbol.(names(df))
    if !(colid() in dfcols)
        error("Die Eingabetabelle enthält keine passende Spalte für die ID (ist: `$(colid())`). "
              * "Siehe `setcolnameid`")
    end

    # Prüfe Vergleichsspalten
    mcols = intersect(MATCHCOLS, dfcols)
    occmcols = intersect(MATCHOCCCOLS, Symbol.(names(dfocc)))

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
    @info "Abfragespalten" querycols

    dictquery = Dict("name" => "",
                     "ort" => "")

    dbest = Dict{Int, Int}()
    irow = 1
    for row in eachrow(df)
        irow % nmsg == 0 && println("Datensatz: ", irow)
        records = QRecord[]
        nbest = 0
        ffound = false
        currentid = row[colid()]

        if row[:Vorname] == ""
            # kein gültiger Eintrag
            @warn "Kein Vorname für (keine Abfrage): " currentid
        else
            try

                # Die Abfrage nach Amt müsste mehrfach durchgeführt werden,
                # weil ein Amt unter verschiedenen Bezeichnungen in der Personendatenbank
                # erfasst sein kann.
                # Schränke nur nach dem Ort ein und werte die allfällige größere Trefferliste aus.

                # Abfrage nach Name und Amtsort/Bistum

                dictquery["name"] = if :Familienname in querycols && row[:Familienname] != ""
                    row[:Vorname] * " " * row[:Familienname]
                else
                    row[:Vorname]
                end

                # Ämter für row
                rowsocc = Util.rowselect(dfocc, currentid, colid())

                places = String[]
                if :Amtsort in querycols                   
                    places = filter(!ismissing, rowsocc[!, :Amtsort])
                end

                # Wir fragen für einen Datensatz in `df` mehrmals ab.
                # Ein Referenzdatensatz kann mehrmals auftauchen.
                # Eine mehrmalige Prüfung in `evaluate!` wird vermieden.
                listchecked = String[] 
                # Argumente für evalutate!
                evalargs = (listchecked, row, rowsocc, toldateofdeath, toloccupation, mcols, occmcols)

                if length(places) > 0
                    for place in places
                        dictquery["ort"] = place
                        gsres = getGS(URLGSINDEX, dictquery)
                        @debug listchecked
                        append!(records, evaluate!(gsres, evalargs...))
                    end
                else
                    delete!(dictquery, "ort")
                    gsres = getGS(URLGSINDEX, dictquery)
                    records = evaluate!(gsres, evalargs...)
                end

                if length(records) > 0
                    bestrec, posbest = findmax(records)
                    if !isless(bestrec, minscore)
                        nbest = writematch!(row, bestrec, records)
                    else
                        with_logger(filelog) do
                            bestkey = bestrec["muster"]
                            @info "abgelehnt" currentid  bestkey
                        end
                    end
                end
                dbest[nbest] = get(dbest, nbest, 0) + 1

        catch
            @warn "Fehler bei Datensatz: " currentid
            rethrow()
            end
        end
        irow += 1
    end

    if logpath != ""
        flush(logio)
        close(logio)
    end
    return dbest
end

# reconcilebyname!
# obsolet: Kann über das Argument querycols in reconcile erreicht werden.
# gelöscht 2020-07-09

reconcile!(df::AbstractDataFrame, field::Pair{Symbol, String}; nmsg = 40) = reconcile!(df, tuple(field), nmsg)

function reconcile!(df::AbstractDataFrame, fields; nmsg = 40)
    coltokey = Dict(col => kgs for (col, kgs) in fields)

    dictquery = Dict(p.second => "" for p in coltokey)
    irow = 0
    for row in eachrow(df)
        (irow += 1) % nmsg == 0 && println("Datensatz: ", irow)

        for (col, kgs) in coltokey
            qv = row[col]
            if !Util.hasdata(qv)
                delete!(dictquery, kgs)
            else
                dictquery[kgs] = qv
            end
        end
        @infiltrate
        if isempty(dictquery) continue end

        gsres = getGS(URLGS, dictquery)

        records = gsres["records"]
        n = length(records)
        if n > 0
            theone = records[1]
            lastocc = GSOcc.getlastocc(theone)
            if !isnothing(lastocc)
                theone["amt"] = lastocc
            end
            writerow!(row, theone, n)
        end
    end
    
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
            row[coldst] = gsn = ares[1]["item.gsn"][1]["nummer"]
        elseif nres > 1
            @warn "Mehrere Treffer für" gndnumber
        end
    end
    gsn
end



"""
     evaluate!(gsres::Dict{String, Any}, listchecked, row, rowsocc, toldod, tolocc, mcols, occmcols)

Bewerte die Datensätze in `gsres` anhand der Werte in `row` und in den Zeilen `rowsocc`.
`listchecked`: Liste von IDs für Personen, die für `row` schon einmal geprüft wurden.
`toldod`: Toleranz Sterbedatum
`tolocc`: Toleranz Amtsbeginn, Amtsende
`mcols`: Vergleichspalten zur Person in den Abfragedaten
`occmcols`: Vergleichsspalten zum Amt in den Abfragedaten
"""
function evaluate!(gsres::Dict{String, Any}, listchecked, row, rowsocc, toldod, tolocc, mcols, occmcols)
    records = QRecord[]
    nvalid = 0
    matchkey = ""
    sreject = String[]

    for record in gsres["records"]
        idgs = record["person"]["id"]
        if idgs in listchecked
            continue
        else
            push!(listchecked, idgs)
        end
        # println(record["preferredName"], ": mk: ", mk, " scdod: ", scdod)

        # Die Bewertung wird in `record` in den Feldern `muster` und `zeitguete`
        # abgelegt.
        record["amuster"] = String[]
        record["zeitguete"] = 0
        evaluategnfn!(record, row, mcols)
        evaluatedod!(record, row, toldod, mcols)

        # Wenn die Sterbedaten vorhanden sind, aber außerhalb der Toleranz
        # liegen, lehne den Datensatz ab.
        if record["zeitguete"] == -1
            push!(sreject, join(record["amuster"], " "))
            continue
        end

        GSOcc.evaluate!(record, rowsocc, tolocc, occmcols)

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

# Umgebung für evaltuatedod!
let

    ryear = "[0-9]?[0-9]?[0-9]{2}"
    rgxyear = Regex(ryear)
    rgxdobad = Regex("(" * ryear * ")"
                     * " *((-|bis)[^1-9]*"
                     * "(" * ryear * "))?")

    function matchdod(sdodqd::Union{<:AbstractString, Missing}, sdodcand, toldod)
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

    function matchdod(dodqd::T, sdodcand, toldod) where T<:Real
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

    """
        evaluatedod!(record, row, toldod, mcols)

    Verwende `toldod` für die Bewertung des Wertes in `:Sterbedatum`.
    Wenn beide Daten vorhanden sind, aber außerhalb der Toleranz liegen,
    schreibe in das Feld "zeitguete" der Wert -1.
    `mcols`: Spalten in den Abfragedaten
    """
    global evaluatedod!
    function evaluatedod!(record, row, toldod, mcols)::Nothing
        # Für die Bischöfe vor 1198 gibt es oft nur eine Angabe für das
        # Jahrhundert. "[4. Jh.]"

        # rgxjh = r"([1-9][0-9]?)\. Jh" # wird im Moment nicht weiter verfolgt

        matchkey, score = "", 0
        if :Sterbedatum in mcols
            matchkey, score = matchdod(row[:Sterbedatum],
                                       record["person"]["sterbedatum"],
                                       toldod)

            if matchkey != ""
                push!(record["amuster"], matchkey)
                record["zeitguete"] = score
            end
        end
        return nothing
    end

end

"""
    evaluategnfn!(record, row, mcols)

Suche nach Familienname und oder Vorname
`qcol`: Spalten in den Abfragedaten
"""
function evaluategnfn!(record, row, mcols)

    stripsplit(a) = strip.(split(a, r",|;"))

    fnqd = [row[:Familienname]]
    if :Familiennamenvarianten in mcols
        fnqdalt = row[:Familiennamenvarianten]
        if Util.hasdata(fnqdalt)
            append!(fnqd, stripsplit(fnqdalt))
        end
    end

    fngs = [record["person"]["familienname"]]
    fngsalt = record["person"]["familiennamenvarianten"]
    if Util.hasdata(fngsalt)
        append!(fngs, stripsplit(fngsalt))
    end

    gnqd = [row[:Vorname]]
    if :Vornamenvarianten in mcols
        gnqdalt = row[:Vornamenvarianten]
        if Util.hasdata(gnqdalt)
            append!(gnqd, stripsplit(gsqdalt))
        end
    end

    gngs = [record["person"]["vorname"]]
    gngsalt = record["person"]["vornamenvarianten"]
    if Util.hasdata(gngsalt)
        append!(gngs, stripsplit(gngsalt))
    end

    ffn = findcommonelement(Util.checkname, fnqd, fngs)
    fgn = findcommonelement(Util.checkname, gnqd, gngs)

    matchkey = ["fn", "vn"][[ffn, fgn]]

    if matchkey != String[]
        append!(record["amuster"], matchkey)
    end
    return matchkey
end

"""
    findcommenelement(f, nqd, ngs)

Prüfe, ob `nqd` und `ngs` mindestens ein übereinstimmendes Elementepaar haben.
"""
function findcommonelement(f, nqd, ngs)::Bool
    fcommon = false
    for cnqd in nqd, cngs in ngs
        if f(cnqd, cngs)
            fcommon = true
            break
        end
    end
    return fcommon
end

function writematch!(row, bestrec, records)

    nbest = count(isequal(bestrec), records)
    writerow!(row, bestrec, nbest)
    if nbest > 1
        with_logger(filelog) do
            @info "mehrere Treffer:" row[colid()] bestrec["muster"]
        end
    end
    nbest
end

writerow!(row, record::QRecord, nbest = 1) = writerow!(row, record.data, nbest)
        
"""
    writerow!(row, node, nbest = 1)

`node` ist ein Datensatz aus der Liste "records" einer GS-Abfrage.
"""
function writerow!(row, node::Dict{String, Any}, nbest = 1)
    agsn = String[]
    for item in node["item.gsn"]
        push!(agsn, item["nummer"])
    end

    # Finde die erste GSN
    gsn1 = (pdelim = (findfirst(',', agsn[1]))) == nothing ? agsn[1] : agsn[1][1:(pdelim - 1)]
    
    row[:GSN1_GS] = gsn1
    row[:GSN_GS] = join(agsn, ", ")
    row[:ID_GND_GS] = node["person"]["gndnummer"]
    if haskey(node, "muster")
        matchkey = node["muster"]
        row[:Qualitaet_GS] = matchkey
        row[:QRang_GS] = Rank.getrank(matchkey)
    end
    row[:nTreffer_GS] = nbest
    row[:Vorname_GS] = node["person"]["vorname"]
    row[:Vornamenvarianten_GS] = node["person"]["vornamenvarianten"]
    row[:Namenspraefix_GS] = node["person"]["namenspraefix"]
    row[:Familienname_GS] = node["person"]["familienname"]
    row[:Familiennamenvarianten_GS] = node["person"]["familiennamenvarianten"]
    row[:Namenszusatz_GS] = node["person"]["namenszusatz"]
    row[:Geburtsdatum_GS] = node["person"]["geburtsdatum"]
    row[:Sterbedatum_GS] = node["person"]["sterbedatum"]
    if haskey(node, "amt") # eines der gesuchten Ämter
        row[:Amt_GS] = node["amt"]["bezeichnung"]
        row[:Amtsbeginn_GS] = node["amt"]["von"]
        row[:Amtsende_GS] = node["amt"]["bis"]
        row[:Dioezese_GS] = node["amt"]["dioezese"]
    end
    row[:Aemter_GS] = getocc(node)[1]
    # Ämter gibt es in GS mehrere
end

function getocc(node)
    aocc = String[]
    aoccplace = String[]
    if haskey(node, "aemter")
        for occrec in node["aemter"]
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
    # `insertcols!` scheint nicht einfacher
    for col in GSSTRINGCOLS
        dfgs[!, col] .= ""
    end
    dfgs[!, :nTreffer_GS] .= 0
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


end # GSquery
