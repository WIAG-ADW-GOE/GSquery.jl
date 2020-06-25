# GSquery
Ergänze Personendaten in Tabellenform um Angaben aus dem digitalen Personenregister
des Projektes Germania Sacra.

Bewerte dabei die gefundenen Datensätze nach der Übereinstimmung mit Feldern in den Abfragedaten.

Angaben zu Amtsdaten werden nur dann als übereinstimmend gewertet, wenn der Typ des
Amtes in einer konfigurierbaren Liste von Ämtern vorkommt.

[Projektseite der Germania Sacra](https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/)

[Digitales Personenregister](http://germania-sacra-datenbank.uni-goettingen.de/)

API: http://personendatenbank.germania-sacra.de/api/v1.0/person

[API Beschreibung](https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/schnittstellen-und-linked-data/)

Das Paket ist zur Verwendung mit [Julia](https://julialang.org/) vorgesehen. 

## Inhalt

* [Verwendung](#verwendung)
* [Eingangsdaten](#eingangsdaten)
* [Ausgabedaten](#ausgabedaten)
* [Parameter](#parameter)
* [Funktionen](#funktionen)
* [Verschiedenes](#verschiedenes)

## Verwendung

Installiere das Paket, z.B. mit
`git clone https://github.com/WIAG-ADW-GOE/GSquery.jl`

``` julia
julia> using Pkg
julia> Pkg.add("[Pfad zu GSquery]/GSQuery")

julia> using DataFrames
```

Lege Abfragedaten fest.
``` julia
julia> dfpersonen = DataFrame(ID_Bischof = [2522, 4446, 3760, 4609, 3580],
                       Praefix = ["Graf von", "", "von", "", "von"],
                       Vorname = ["Hartmann", "Herwig", "Johann", "Pilgrim", "Johann"],
                       Familienname = ["Dillingen", "", "Egloffstein", "", "Sierck"],
                       Sterbedatum = ["1286", "", "1411", "", "1305"])

5×5 DataFrame
│ Row │ ID    │ Praefix  │ Vorname  │ Familienname │ Sterbedatum │
│     │ Int64 │ String   │ String   │ String       │ String      │
├─────┼───────┼──────────┼──────────┼──────────────┼─────────────┤
│ 1   │ 2522  │ Graf von │ Hartmann │ Dillingen    │ 1286        │
│ 2   │ 4446  │          │ Herwig   │              │             │
│ 3   │ 3760  │ von      │ Johann   │ Egloffstein  │ 1411        │
│ 4   │ 4609  │          │ Pilgrim  │              │             │
│ 5   │ 3580  │ von      │ Johann   │ Sierck       │ 1305        │


julia> dfaemter = DataFrame(ID_Bischof = ["2522", "3580", "3580", "3760", "3760", "4446", "4609"],
                     Bistum = ["Augsburg", "Utrecht", "Toul", "Würzburg", "Würzburg", "Meißen", "Ölmütz"],
                     Amtsart = ["Bischof", "Bischof", "Bischof", "Koadjutor des Bischofs", "Bischof", "Bischof", "Bischof"],
                     Amtsbeginn = ["1248", "1291", "1296", "1396", "1400", "1106", "1182"],
                     Amtsende = ["1286", "1296", "1305", "1400", "1411", "1119", "1184"])

7×5 DataFrame
│ Row │ ID_Bischof │ Bistum   │ Amtsart                │ Amtsbeginn │ Amtsende │
│     │ String     │ String   │ String                 │ String     │ String   │
├─────┼────────────┼──────────┼────────────────────────┼────────────┼──────────┤
│ 1   │ 2522       │ Augsburg │ Bischof                │ 1248       │ 1286     │
│ 2   │ 3580       │ Utrecht  │ Bischof                │ 1291       │ 1296     │
│ 3   │ 3580       │ Toul     │ Bischof                │ 1296       │ 1305     │
│ 4   │ 3760       │ Würzburg │ Koadjutor des Bischofs │ 1396       │ 1400     │
│ 5   │ 3760       │ Würzburg │ Bischof                │ 1400       │ 1411     │
│ 6   │ 4446       │ Meißen   │ Bischof                │ 1106       │ 1119     │
│ 7   │ 4609       │ Ölmütz   │ Bischof                │ 1182       │ 1184     │
```

Gib den Namen der Spalte an, in der die ID gespeichert ist und welche die beiden
Tabellen verknüpft.
``` julia
julia> GSquery.setcolnameid(:ID_Bischof)
```

Erzeuge die Ausgabetabelle.
``` julia
julia> dfpersonengs = GSquery.makeGSDataFrame(dfpersonen);
```

Frage das digitale Personenregister ab und zeige das Ergebnis an.
```julia
julia> GSquery.reconcile!(dfpersonengs, dfaemter; nmsg=2)

┌ Info: Level:
│   minkey = "vn ae ab ao"
└   minrank = 60
┌ Info: Gültige Ämter
│   GSOcc.occupations =
│    6-element Array{String,1}:
│     "Bischof"
│     "Vikar"
│     "Elekt"
│     "Administrator"
│     "Patriarch"
└     "Metropolit"
Datensatz: 2
Datensatz: 4
Dict{Int64,Int64} with 2 entries:
  0 => 1
  1 => 4
  
julia> dfpersonengs[!, [:Vorname, :Familienname, :Sterbedatum, :Qualitaet_GS, :QRang_GS, :GSN1_GS, :Dioezese_GS, :Aemter_GS]]

5×8 DataFrame
│ Row │ Vorname  │ Familienname │ Sterbedatum │ Qualitaet_GS         │ QRang_GS │ GSN1_GS       │ Dioezese_GS │ Aemter_GS                │
│     │ String   │ String       │ String      │ String               │ Int64    │ String        │ String      │ String                   │
├─────┼──────────┼──────────────┼─────────────┼──────────────────────┼──────────┼───────────────┼─────────────┼──────────────────────────┤
│ 1   │ Hartmann │ Dillingen    │ 1286        │ fn sd vn ae ab ao at │ 1        │ 053-01059-001 │ Augsburg    │ Bischof                  │
│ 2   │ Herwig   │              │             │ vn ae ab ao at       │ 29       │ 046-03578-001 │ Meißen      │ Bischof                  │
│ 3   │ Johann   │ Egloffstein  │ 1411        │ fn sd vn ae ab ao at │ 1        │ 059-00935-001 │ Würzburg    │ Bischof, Dompropst       │
│ 4   │ Pilgrim  │              │             │                      │ 199      │               │             │                          │
│ 5   │ Johann   │ Sierck       │ 1305        │ fn sd vn ae ab ao at │ 1        │ 029-02358-001 │ Toul        │ Bischof, Bischof, Propst │  
  
```

Die Spalte `Qualitaet_GS` gibt an, wie gut die Übereinstimmung des Treffers mit den
angegebenen Daten ist. Dabei steht jedes Kürzel für ein übereinstimmendes Datenfeld:
* `fn`: Familiename
* `vn`: Vorname
* `sd`: Sterbedatum (Jahr)
* `at`: Amtsart
* `ab`: Amtsbeginn
* `ae`: Amtsende
* `ao`: Amtsort (Bistum)

Alternativ: lies die Daten aus Dateien ein; Beispieldaten: [`personen.tsv`](./data/personen.tsv), 
[`aemter.tsv`](./data/aemter.tsv).

```julia
julia> using FileIO
julia> using CSVFiles

julia> dateipersonen = File(format"CSV", "personen.tsv")
julia> dfpersonen = DataFrame(load(dateipersonen, delim = '\t'))


julia> dateiaemter = File(format"CSV", "aemter.tsv")
julia> dfaemter = DataFrame(load(dateiaemter, delim = '\t'))
```

Erzeuge die Ausgabetabelle
```julia
julia> GSquery.setcolnameid(:ID) # nur nötig, falls die Spalte nicht 'ID' heißt.
julia> dfpersonengs = GSquery.makeGSDataFrame(dfpersonen)
```

Befülle die Ausgabetabelle
```julia
julia> GSquery.reconcile!(dfpersonengs, dfaemter)
```

Zeige das Ergebnis an
```
julia> dfpersonengs[!, [:ID, :Vorname, :Familienname, :Sterbedatum, :Qualitaet_GS, :QRang_GS, :GSN1_GS, :Dioezese_GS, :Aemter_GS]]

│ Row │ ID    │ Vorname   │ Familienname │ Sterbedatum │ Qualitaet_GS         │ QRang_GS │ GSN1_GS       │ Dioezese_GS │ Aemter_GS                │
│     │ Int64 │ String    │ String       │ Int64?      │ String               │ Int64    │ String        │ String      │ String                   │
├─────┼───────┼───────────┼──────────────┼─────────────┼──────────────────────┼──────────┼───────────────┼─────────────┼──────────────────────────┤
│ 1   │ 3273  │ Otto      │ Rietberg     │ 1307        │ fn sd vn ae ab ao at │ 1        │ 066-04327-001 │ Paderborn   │ Bischof                  │
│ 2   │ 5286  │ Georg     │ Altdorfer    │ 1495        │ fn sd ae ab ao at    │ 6        │ 019-01008-001 │ Chiemsee    │ Bischof                  │
│ 3   │ 4247  │ Rudhart   │              │ missing     │ vn ae ab ao at       │ 29       │ 056-01429-001 │ Konstanz    │ Bischof                  │
│ 4   │ 3474  │ Friedrich │ Schwerin     │ 1239        │ fn vn ae ab ao at    │ 7        │ 060-01169-001 │ Schwerin    │ Dompropst, Bischof       │
│ 5   │ 2522  │ Hartmann  │ Dillingen    │ 1286        │ fn sd vn ae ab ao at │ 1        │ 053-01059-001 │ Augsburg    │ Bischof                  │
│ 6   │ 4446  │ Herwig    │              │ missing     │ vn ae ab ao at       │ 29       │ 046-03578-001 │ Meißen      │ Bischof                  │
│ 7   │ 3760  │ Johann    │ Egloffstein  │ 1411        │ fn sd vn ae ab ao at │ 1        │ 059-00935-001 │ Würzburg    │ Bischof, Dompropst       │
│ 8   │ 4609  │ Pilgrim   │              │ missing     │                      │ 199      │               │             │                          │
│ 9   │ 3580  │ Johann    │ Sierck       │ 1305        │ fn sd vn ae ab ao at │ 1        │ 029-02358-001 │ Toul        │ Bischof, Bischof, Propst │
│ 10  │ 20796 │ Johannes  │ Tiedemann    │ 1561        │                      │ 199      │               │             │                          │
│ 11  │ 4610  │ Kaim      │              │ missing     │                      │ 199      │               │             │                          │
│ 12  │ 4234  │ Johannes  │              │ missing     │ vn ae ab ao at       │ 29       │ 056-00832-001 │ Konstanz    │ Bischof, Abt, Abt        │
│ 13  │ 4141  │ Markward  │              │ missing     │ vn ae ab ao at       │ 29       │ 030-03731-001 │ Hildesheim  │ Bischof                  │
```

Speichere das Ergebnis
```julia
julia> save("personen_gs.tsv", dfpersonengs)
``` 

## Eingangsdaten

Personentabelle mit mindestens folgenden Spalten
* `ID` (Der Name der Spalte, in der die ID erwartet wird, kann mit `setcolnameid` gesetzt werden)
* `Vorname`
* `Familienname`
* `Sterbedatum`

Beispiel [`personen.tsv`](./data/personen.tsv)

Es müssen nicht alle Felder befüllt sein, zumindest aber das Feld `Vorname`.

| ID | Vorname | Familienname | Sterbedatum |
| -- | ------- | ------------ | ----------- |
| 3273 | Otto | Rietberg | 1307 |
| 5286 | Georg | Altdorfer | 1495 |
| 4247 | Rudhart |  |  |
| 3474 | Friedrich | Schwerin | 1239 |
| 2522 | Hartmann | Dillingen | 1286 |
| 4446 | Herwig |  |  |
| 3760 | Johann | Egloffstein | 1411 |
| 4609 | Pilgrim |  |  |
| 3580 | Johann | Sierck | 1305 |
| 20796 | Johannes | Tiedemann | 1561 |
| 4610 | Kaim |  |  |
| 4234 | Johannes |  |  |
| 4141 | Markward | | |

Beispiel [`aemter.tsv`](./data/aemter.tsv)

| Bistum | Amtsart | Amtsbeginn | Amtsende | ID_Amt | ID |
| -- | ------- | ------------ | ----------- | --- | --- |
| Augsburg | Bischof | 1248 | 1286 | 5 | 2522 |
| Paderborn | Elekt | 1277 | 1279 | 3586 | 3273 |
| Paderborn | Bischof | 1279 | 1307 | 715 | 3273 |
| Schwerin | Bischof | 1238 | 1239 | 903 | 3474 |
| Utrecht | Bischof | 1291 | 1296 | 3756 | 3580 |
| Toul | Bischof | 1296 | 1305 | 1002 | 3580 |
| Würzburg | Koadjutor des Bischofs | 1396 | 1400 | 3869 | 3760 |
| Würzburg | Bischof | 1400 | 1411 | 1174 | 3760 |
| Hildesheim | Bischof | 874 | 880 | 1541 | 4141 |
| Konstanz | Bischof | 760 | 782 | 1631 | 4234 |
| Konstanz | Bischof | 1018 | 1022 | 1644 | 4247 |
| Meißen | Bischof | 1106 | 1119 | 1834 | 4446 |
| Ölmütz | Bischof | 1182 | 1184 | 1991 | 4609 |
| Ölmütz | Bischof | 1186 | 1194 | 1992 | 4610 |
| Chiemsee | Bischof | 1477 | 1495 | 2644 | 5286 |
| Lübeck | Bischof | 1559 | 1561 | 21447 | 20796 |


Ämtertabelle mit mindestens folgenden Spalten
* `ID` (ID für die Person, Referenz auf die Personentabelle
* `Amtsart`
* `Amtsbeginn`
* `Amtsende`
* `Bistum`

## Ausgabedaten

Die Ausgabe ist eine erweiterte Personentabelle mit mindestens folgenden Spalten.
* `ID_Bischof`
* `Praefix`
* `Vorname`
* `Familienname`
* `Sterbedatum`
* `GSN1_GS`
* `GSN_GS`
* `ID_GND_GS`
* `Qualitaet_GS`
* `nTreffer_GS`
* `Vorname_GS`
* `Vornamenvarianten_GS`
* `Namenspraefix_GS`
* `Familienname_GS`
* `Familiennamenvarianten_GS`
* `Namenszusatz_GS`
* `Geburtsdatum_GS`
* `Sterbedatum_GS`
* `Amtbischof_GS`
* `Amtsbeginn_GS`
* `Amtsende_GS`
* `Dioezese_GS`
* `Aemter_GS`
* `Dioezesen_GS`
* `QRang_GS`


## Parameter

---

```julia
	setcolnameid(id)

```

Setze den Namen der Spalte, welche die ID enthält

Voreinstellung: `:ID`

---

```julia
setminmatchkey(matchkey::AbstractString)
```

Setze den Parameter für den Mindestwert an Übereinstimmung

`matchkey`: Muster, das noch als Treffer ausgegeben wird.

Treffer mit einem schlechterem Muster als `matchkey` werden nicht berücksichtigt.

Beispiel

`"fn vn"`: Mindestens Familienname und Vorname müssen übereinstimmen.

---

```julia
GSOcc.setoccupations(occ)
```
Setze die Liste der Ämter, die bei der Abfrage in Betracht gezogen werden.

`setoccupations()`: Gib die aktuelle Liste aus.

Beispiel

```julia
setoccupations(["Pfarrer", "Vikar"])
```

---

```julia
GSOcc.setequivalentoccupations(lequivocc)

```

Setze Liste von Zuordnungen für als gleichwertig betrachtete Ämter.

Beispiel

```julia
GSOcc.setequivalentoccupations(["Gewählter Bischof" => "Elekt", 
                          "Erwählter Bischof" => "Elekt"])
```

---

`setlogpath(logfile::AbstractString)`

Setze den Namen der Datei für Logdaten

Wenn der Pfad der Logdatei leer ist (""), werden keine Logdaten geschrieben. 
`setlogpath()` gibt den aktuellen Pfad der Logdatei aus.

---

```julia
Rank.setfileofranks(fileranks::AbstractString)
```

Setze den Dateinamen für die Liste mit Übereinstimmungsmustern.

Wenn die entsprechende Datei nicht vorhanden ist, wird für die Abfrage 
automatisch eine Rangliste von Mustern erstellt.

`setfileofranks()`: Gib den aktuellen Dateinamen aus.

---

```julia
Rank.setdrank(fileranks = Rank.fileranks)
```

Lies eine Liste mit Übereinstimmungsmustern aus `fileranks`.

`Rank.setdrank()` erzeugt eine Standardversion der Liste von Übereinstimmungsmustern.

---

```julia
Rank.ranklist()
```

Gib die Liste mit Übereinstimmungsmustern aus.

---

```julia
setinputcols(cols)
```

Setze die Namen der Spalten der Eingabetabelle, die in die Ausgabetabelle übernommen werden sollen

`setinputcols()`: Gib die Spaltennamen aus.

---


## Funktionen

```julia
reconcile!(df::AbstractDataFrame,
           dfocc::AbstractDataFrame,
           nmsg = 40,
           toldateofdeath = 2,
           toloccupation = 2)
```

Vergleiche die gefundenen Datensätze mit Name, Ort und Amt aus dem Abfragedatensatz.

Ergänze `df` für jeden Datensatz mit den Daten aus dem besten Treffer.
Gib nach einer Zahl von `nmsg` Datensätzen eine Fortschrittsmeldung aus.
Verwende `toldateofdeath` als Toleranz für das Sterbedatum und
`toloccupation` als Toleranz für Amtsdaten.
Für Testdaten kann eine View auf `df` übergeben werden.

---

## Verschiedenes
Wenn die Verbindung zum Server unterbrochen wird und wieder zustandekommt, wird die 
die Abfrageschleife unter Umständen nicht wieder aufgenommen. Schleife mit
Ctrl-C abbrechen und neu starten oder mit dem Teil der Daten aufrufen, die noch nicht
bearbeitet sind.

Der Typ der ID in der Personentabelle und der Ämtertabelle sollte übereinstimmen,
z.B. `String`.

