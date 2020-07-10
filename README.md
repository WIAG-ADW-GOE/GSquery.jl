# GSquery
Ergänze Personendaten in Tabellenform um Angaben aus dem digitalen Personenregister
des Projektes Germania Sacra.

Das Personenregister kann in zwei verschiednen Modi abgefragt werden. Im ersten Modus
gibt die Benutzerin bestimmte Felder an und `GSquery` liest Daten aus dem
Personenregister aus, wenn die Suche mindestens einen Treffer ergab.
Im zweiten Modus fragt `GSquery` das Personenregister nach vorgegebenen
Namensbestandteilen oder dem Amtsort an. 
Für jeden Datensatz in den
Anfragedaten bewertet `GSquery` die vom Personenregister erhaltene Trefferliste.
Folgende Feldnamen in den Anfragedaten werden
berücksichtigt, wenn sie vorhanden sind. 

* Vorname
* Vornamenvarianten
* Familienname
* Familiennamenvarianten
* Sterbedatum
* Amtsart
* Amtsbeginn
* Amtsende
* Amtsort

So wird einerseits erreicht, dass jeweils
der beste Treffer oder ein bester Treffer ausgegeben wird, andererseits kann die
Trefferliste bewertet und gegebenenfalls gefiltert werden. 
Die Benutzerin kann aber auch schon von vornherein über ein Mindestkriterium einen
Filter festlegen, und so Treffer ausschließen, die diesem Mindestkriterium nicht
entsprechen.

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
julia> using GSquery

julia> using DataFrames
```
### Abgleich über bestimmte Felder
Die Benutzerin gibt an, wie die Felder in den Abfragedaten auf die Felder im
digitalen Personenregister abgebildet werden sollen.

Beispiele
``` julia
(:Nachname => "person.familienname",
 :Amtsart => "amt.bezeichnung")
 
(:GND_ID => "person.gndnummer",)	

```

#### Beispielabfrage
Lege Abfragedaten fest.
``` julia
julia> dfpersonen = DataFrame(ID_Bischof = [2522, 4446, 3760, 4609, 3580],
                              Praefix = ["Graf von", "", "von", "", "von"],
                              Vorname = ["Hartmann", "Herwig", "Johann Philipp", "Pilgrim", "Johann"],
                              Familienname = ["Dillingen", "", "Egloffstein", "", "Sierck"],
                              Sterbedatum = ["1286", "", "1411", "", "1305"],
							  Amtsart = ["Bischof", "Bischof", "Domherr", "", "Bischof"])
```

Gib den Feldnamen für die ID bekannt und erzeuge die Ausgabetabelle.
``` julia
GSquery.setcolnameid(:ID_Bischof)
dfpersonengs = GSquery.makeGSDataFrame(dfpersonen);
```

Lege die Felder der Abfrage fest und befülle die Ausgabetabelle.
``` julia
fields = (:Vorname => "person.vorname",
          :Familienname => "person.familienname",
          :Amtsart => "amt.bezeichnung")
GSquery.reconcile!(dfpersonengs, fields, nmsg = 2)

```

Befülle die Ausgabetabelle
``` julia
GSquery.reconcile!(dfpersonengs, fields, nmsg = 2)
```

Ergebnis
``` julia
julia> dfpersonengs[!, [:ID_Bischof, :Vorname, :Familienname, :Qualitaet_GS, :QRang_GS, :GSN1_GS, :Dioezese_GS, :Aemter_GS, :nTreffer_GS]]
5×9 DataFrame
│ Row │ ID_Bischof │ Vorname  │ Familienname │ Qualitaet_GS │ QRang_GS │ GSN1_GS       │ Dioezese_GS │ Aemter_GS                │ nTreffer_GS │
│     │ Int64      │ String   │ String       │ String       │ Int64    │ String        │ String      │ String                   │ Int64       │
├─────┼────────────┼──────────┼──────────────┼──────────────┼──────────┼───────────────┼─────────────┼──────────────────────────┼─────────────┤
│ 1   │ 2522       │ Hartmann │ Dillingen    │              │ 199      │ 053-01059-001 │ Augsburg    │ Bischof                  │ 1           │
│ 2   │ 4446       │ Herwig   │              │              │ 199      │ 046-03578-001 │ Meißen      │ Bischof                  │ 1           │
│ 3   │ 3760       │ Johann   │ Egloffstein  │              │ 199      │               │             │                          │ 0           │
│ 4   │ 4609       │ Pilgrim  │              │              │ 199      │ 010-02218-001 │ Köln        │ Mönch                    │ 22          │
│ 5   │ 3580       │ Johann   │ Sierck       │              │ 199      │ 029-02358-001 │ Toul        │ Bischof, Bischof, Propst │ 1           │

```

Die Felder müssen für die Abfrage nicht alle befüllt sein.

In der
[Dokumentation](https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/schnittstellen-und-linked-data/)
des digitalen Personenregisters sind die Felder beschrieben, die abgefragt werden
können.

### Abgleich mit Bewertung der Ergebnisse
Die Benutzerin gibt eine Liste von Feldbezeichnungen an, die als Suchparameter an das
digitale Personenregister gesendet werden. Typischerweise ist diese Liste eine Kombination aus
Namensbestandteilen und Amtsort.

Beispiele:
```julia
[:Vorname, :Familienname]
[:Vorname, :Familienname, :Amtsort]
[:Familienname. :Amtsort]
```


Es kann eine Liste mit Amtsbezeichnungen festgelegt werden. In diesem Fall werden
Angaben zu Amtsdaten nur dann als übereinstimmend gewertet, wenn die
Amtsbezeichnung aus der Personendatenbank in dieser Liste vorkommt. 

#### Beispielabfrage
Lege Abfragedaten fest als Tabelle mit Personendaten
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
```

... und zugehörigen Amtsdaten. Verwende den Inhalt von `:Bistum` als `:Amtsort`.
``` julia


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

julia> rename!(dfaemter, :Bistum => :Amtsort)
```

Lege die Liste gültiger Ämter fest.
``` julia
julia> GSquery.setoccupations(["Bischof", "Vikar", "Elekt", "Administrator", "Patriarch", "Metropolit"])
```

Gib den Feldnamen für die ID an, über welche die beiden Tabellen verknüpft.
``` julia
julia> GSquery.setcolnameid(:ID_Bischof)
```

Erzeuge die Ausgabetabelle. Verwende den Inhalt von `:Bistum` als `:Amtsort`
``` julia
julia> dfpersonengs = GSquery.makeGSDataFrame(dfpersonen);
julia> rename!(dfaemter, :Bistum => :Amtsort)
```

Lege die Felder für die Abfrage fest. Frage das digitale Personenregister ab und
zeige das Ergebnis an.
```julia
julia> qcols = [:Familienname, :Vorname, :Amtsort]
julia> GSquery.reconcile!(dfpersonengs, qcols, dfaemter; nmsg=2)

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

#### Beispielabfrage mit Daten aus einer Datei
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
julia> qcols = [:Vorname, :Familiename, :Amtsort]
julia> GSquery.reconcile!(dfpersonengs, qcols, dfaemter)
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

Personentabelle mit folgenden Spalten
* `ID` (Der Name der Spalte, in der die ID erwartet wird, kann mit `setcolnameid` gesetzt werden)
* `Vorname`
* `Familienname`
* `Sterbedatum` (optional)

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


Ämtertabelle mit mindestens einer der folgenden Spalten
* `ID` (ID für die Person, Referenz auf die Personentabelle
* `Amtsart`
* `Amtsbeginn`
* `Amtsende`
* `Amtsort`

Beispiel [`aemter.tsv`](./data/aemter.tsv)

| Amtsort | Amtsart | Amtsbeginn | Amtsende | ID_Amt | ID |
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


## Ausgabedaten

Die Ausgabe ist eine erweiterte Personentabelle.

Beispiel für die Liste der Spalten

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

Im Untermodul GSOcc:
```julia
setoccupations(occ)
```
Setze die Liste der Ämter, die bei der Abfrage in Betracht gezogen werden.

`setoccupations()`: Gib die aktuelle Liste aus.

Beispiel

```julia
setoccupations(["Pfarrer", "Vikar"])
```
Es werden nur Datensätze aus der Personendatenbank berücksichtigt, in denen eines der
Ämter gefunden wird. Wenn die Liste leer ist, entfällt diese Einschränkung.

---

Im Untermodul GSOcc:
```julia
setequivalentoccupations(lequivocc)

```

Setze Liste von Zuordnungen für als gleichwertig betrachtete Ämter.

Beispiel

```julia
setequivalentoccupations(["Gewählter Bischof" => "Elekt", 
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
reconcile!(df,
           querycols,
           dfocc,
           nmsg = 40,
           toldateofdeath = 2,
           toloccupation = 2)
```
Frage das digitale Personenregister nach den Spalten in `querycols` ab.
Vergleiche die gefundenen Datensätze mit Name und Amtsdaten aus dem Abfragedatensatz.
Ergänze `df` für jeden Datensatz mit den Daten aus dem besten Treffer.
Gib nach einer Zahl von `nmsg` Datensätzen eine Fortschrittsmeldung aus.
Verwende `toldateofdeath` als Toleranz für das Sterbedatum und
`toloccupation` als Toleranz für Amtsdaten.

``` julia
reconcile!(df, fields; nmsg = 40)
```
Frage das digitale Personenregister nach den Feldern in `fields` ab.
Ergänze `df` für jeden Datensatz mit den Daten aus dem erstbesten Treffer.
Gib nach einer Zahl von `nmsg` Datensätzen eine Fortschrittsmeldung aus.

`fields` ordnet jeder Spalte in `df` einen `Feldnamen` im Personenregister zu.

---

## Verschiedenes
Das Paket ist [Julia](https://julialang.org/) geschrieben. Die Sprache zeichnet sich
durch sehr kurze Rechenzeiten aus. Vor der ersten Ausführung wird aber für jede Funktion
Maschinencode erzeugt, was einige Sekunden dauern kann. Da dieser Prozess aber nur
einmal notwendig ist, entfällt er bei jedem weiteren Aufruf der Funktion.

Wenn die Verbindung zum Server unterbrochen wird und wieder zustandekommt, wird
die Abfrageschleife unter Umständen nicht wieder aufgenommen. Schleife mit
Ctrl-C abbrechen und neu starten oder mit dem Teil der Daten aufrufen, die noch nicht
bearbeitet sind.

