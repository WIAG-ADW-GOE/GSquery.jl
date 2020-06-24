# GSquery
Ergänze Personendaten in Tabellenform um Angaben aus dem digitalen Personenregister
des Projektes Germania Sacra.

[Projektseite der Germania Sacra](https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/)

[Digitales Personenregister](http://germania-sacra-datenbank.uni-goettingen.de/)

[API](http://personendatenbank.germania-sacra.de/api/v1.0/person)

[API Beschreibung](https://adw-goe.de/forschung/forschungsprojekte-akademienprogramm/germania-sacra/schnittstellen-und-linked-data/)

## Übersicht

Das Paket ist zur Verwendung mit [Julia](https://julialang.org/) vorgesehen. Beachte,
dass beim ersten Aufruf einer Funktion etwas mehr Zeit vergeht, da sie zu diesem
Zeitpunkt kompiliert wird. Bei jedem weiteren Aufruf ist die Ausführungszeit in der
Regel sehr kurz.

## Inhalt

* [Verwendung](#verwendung)
* [Eingangsdaten](#eingangsdaten)
* [Ausgabedaten](#ausgabedaten)
* [Einschränkungen](#einschr-nkungen)

## Verwendung

Installiere das Paket
``` julia
using Pkg
Pkg.add()
```

Lade weitere nützliche Pakete
```julia
using FileIO
using CSVFiles
using DataFrames
```

Lies die Daten ein
```julia
dateipersonen = File(format"CSV", "personen.tsv")
dfpersonen = DataFrame(load(dateipersonen, delim = '\t'))


dateiaemter = File(format"CSV", "aemter.tsv")
dfaemter = DataFrame(load(dateiaemter, delim = '\t'))
```

Erzeuge die Ausgabetabelle
```julia
GS.setcolnameid(:ID) # nur nötig, falls die Spalte nicht 'ID' heißt.
dfpersonengs = GS.makeGSDataFrame(dfpersonen)
```

Befülle die Ausgabetabelle
```julia
GS.reconcile!(dfpersonengs, dfaemter)
```

Zeige das Ergebnis an
```
dfpersonengs[!, [:ID, :Vorname, :Familienname, :Sterbedatum, :Qualitaet_GS, :QRang_GS, :GSN1_GS, :Dioezese_GS, :Aemter_GS]]

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

Die Spalte `Qualitaet_GS` gibt an, wie gut die Übereinstimmung des Treffers mit den
angegebenen Daten ist. Dabei steht jedes kürzel für ein übereinstimmendes Datenfeld:
* `fn`: Familiename
* `vn`: Vorname
* `sd`: Sterbedatum (Jahr)
* `at`: Amtsart
* `ab`: Amtsbeginn
* `ae`: Amtsende
* `ao`: Amtsort (Bistum)


## Eingangsdaten

Personentabelle mit mindestens folgenden Spalten
* `ID` (Der Name der Spalte, in der die ID erwartet wird, kann mit `setcolnameid` gesetzt werden)
* `Vorname`
* `Familienname`
* `Sterbedatum`

Beispiel `personen.txt`

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

Beispiel `aemter.txt`

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



## Funktionen

`setminmatchkey(matchkey::AbstractString)`

Setze den Parameter für den Mindestwert an Übereinstimmung

`matchkey`: Muster, das noch als Treffer ausgegeben wird.

Beispiel

`"fn vn"`

---

`setlogpath(logfile::AbstractString)`

Setze den Namen der Datei für Logdaten

---

```julia
setfileranks(fileranks::AbstractString)
```

Setze den Dateinamen für die Liste mit Übereinstimmungsmustern.

---


# Einschränkungen
Wenn die Verbindung zum Server unterbrochen wird und wieder zustandekommt, wird die
Abfrageschleife unter Umständen nicht wieder aufgenommen.
