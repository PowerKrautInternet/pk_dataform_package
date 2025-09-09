# The pk_dataform_package 
_A data automation package in dataform, developed by PowerKraut._

Current features:
* Data monitoring
* Freshness alerting
* Declaration handling
* Publisher-level recency control (nieuw)

### Deze package draait om twee principes:

1. **Het definiëren van bronnen**
2. **Het definiëren van welke query's je wilt gebruiken**
   

Deze structuur is opgezet om consistent, herbruikbaar en modulair om te gaan met verschillende databronnen en lagen in je datamodel, met gebruik van [Dataform](https://dataform.co/).

---

# 🔧 In gebruik nemen

Query’s worden ingeladen via de package en vervolgens gepubliceerd via standaard Dataform-methodes. Ook bronnen worden centraal gedeclareerd en beschikbaar gesteld aan de rest van de pipeline. Herbruikbare functies, zoals lookups of mappings, zijn beschikbaar via de `functions` map.

## .npmrc
```
@PowerKrautInternet:registry=https://npm.pkg.github.com/
//npm.pkg.github.com/:_authToken=SECRETTOKEN
```
!!Pas wel de secrettoken aan!!

## package.json
```json
{
   "name": "{VUL HIER JE PROJECTNAAM}", 
   "dependencies": {
     "@dataform/core": "3.0.0-beta.6",
     "pk_dataform_package": "https://github.com/PowerKrautInternet/pk_dataform_package/archive/refs/tags/v{VUL HIER DE LAATSTE VERSIE VAN DE PACKAGE}.tar.gz"
   }
}
```

## workflow configuration
met de tag: df_datakwaliteit en een configuration id met "recency" er in. hierdoor weet de alerting policy dat deze meegenomen moet worden met de recency

## Stap 1: Declarations.js
Dit is het bestand dat altijd als eerste uitgevoerd zal worden in uw dataform omgeving. Hier zullen we dan ook alle standaard query's opzetten en onze inkomende bronnen defineren.

Het grootste verschil met de standaard format die dataform aanneemt is dat wij de bronnen eerst verzamelen voordat we ze in een declare functie aanroepen, hierdoor bestaat declerations.js nu uit 3 onderdelen, waarbij je eigelijk alleen de 'const sources' hoeft aan te passen:

```javascript
const sources = [
  {
    // De normale rijen die nodig zijn voor een declare vul je hier ook in. Zoals; database, schema en name.
    database: "dtc-datalake-prod",
    schema: "df_staging_views",
    name: "stg_openRdwData",
    alias: "GA4", // Hier vul je een naam in die meerdere bronnen kan groepen. In dit voorbeeld zou je bijvoorbeeld meerdere bronnen als GA4 kunnen defineren en zo altijd gezamelijk kunnen aanroepen in een ref()
    crm_id: "982", // Hiermee kan je aangeven op welk CRM_ID gefilterd moet worden. (BETA: nog niet overal geimplementeerd)
    key1: "$.type", // Kan je mee aangeven welke key er uit de JSON payload gepakt moet worden voor key1. Dit werkt alleen voor dataproducers.
    account: "Autobedrijf Ede", // Kan je aangeven vanuit welk account deze bron komt, hiermee kan je dus onderscheidt maken tussen meerdere duplicate bronnen die je hebt gelinkt via de alias optie.
    publishers: [               //Je kunt per bron een lijst met publishers meegeven, inclusief een recency-waarde:
      { name: "offertePublisher", recency: false },
      { name: "anderePublisher", recency: true }
    ],
    recency: false, // Je kan ook voor de hele bron de recency uitschakelen
    type: "dataProducer" //wanneer de naam afwijkt maar het wel werkt met een payload.  
  },
  {
    schema: "...",
    name: "...",
    ...
  }
]
let src = require("pk_dataform_package/sources")
let declared = {}
for (let s in sources) {
  declare(sources[s]);
  if(require("pk_dataform_package/sources").getTypeSource(sources[s]) === "dataProducer" && declared[sources[s].name] != true){declare({schema: "df_rawdata_views", name: sources[s].name+"_lasttransaction"}); declared[sources[s].name] = true}
  if(typeof sources[s].name != "undefined" && sources[s].schema == "googleSheets" && declared[sources[s].alias ?? sources[s].name] != true){declare({schema: "df_googlesheets_tables", name: sources[s].alias ?? sources[s].name}); declared[sources[s].alias ?? sources[s].name] = true}
};
src.setSources(sources);
operate("setup_operations", require("pk_dataform_package/setup").setupFunctions(sources))
```
!! Let op: als deze stap word toegevoegd zullen ook automatisch alle "_lasttransation" schema's toegevoegd worden! Dit kan dus voor dubbele schema's zorgen.

## Stap 2: Datakwaliteit.js
In dit bestand moet je de volgende code neerzetten om te zorgen dat je alle datakwaliteit tabellen krijgt:

```javascript
pk = require("pk_dataform_package/queries/df_datakwaliteit");

publish("dk_maxReceivedon",
        {
          type: "table",
          schema: "df_datakwaliteit"
        }
).query(ctx => pk.dk_maxReceivedon());

publish("dk_monitor",
        {
          type: "table",
          schema: "df_datakwaliteit",
          dependencies: "dk_maxReceivedon"
        }
).query(ctx => pk.dk_monitor());

publish("dk_healthRapport",
        {
          type: "incremental",
          schema: "df_datakwaliteit",
          dependencies: "dk_monitor"
        }
).query(ctx => pk.dk_healthRapport());

publish("dk_errormessages",
        {
          type:"table",
          schema: "df_datakwaliteit",
          tags: ["df_datakwaliteit"],
          dependencies: "dk_healthRapport"
        }
).query(ctx => pk.dk_errormessages())
```
Hierbij kan je df_datakwaliteit aanroepen in je workflow configurations.

## Stap 3: Query's en personalisatie
Voor de query stg_facebookdata werkt het nu als volgt:
```javascript
let pk = require("pk_dataform_package/queries/df_staging_views"); 
// let pk... hoef je maar 1x aan te roepen per javascript bestand (.js)

let stg_facebookdata = pk.stg_facebookdata();   // Haal je de laatste versie van de query op (heeft te maken met dependency tracking
publish(                                        // Dataform functie voor het maken van een table/view
    stg_facebookdata.name,                      // Naam van query/table/view
    stg_facebookdata.config                     // Config (zoals deze bovenin een sqlx file staat
).query(                                        
    stg_facebookdata.query                      // De query van de table
)
```
Wanneer het dus gewild is om personalisatie toe te voegen binnen een query is het aangeraden om
- Optie A: Vervang de standaard Query van de table
- Optie B: Vervang de naam van de query en voeg zo een extra stap in het ETL toe
Zoals: het voorbeeld stg_facebookdata_raw noemen en dan een vervolgquery schrijven die je stg_facebookdata noemt. Hierdoor zal de vervolgquery aangeroepen worden in de gestandaardiseerde ETL.
- Optie C: Binnen de package is de mogelijkheid tot SQL injecting toegevoegd. Dit is echter nog in BETA en moeten we over beslissen waar, of en hoe we dit willen implementeren.

---

## 📁 Mappenstructuur

De logica van de query's is gestructureerd in submappen onder `queries/`. Dit helpt om lagen van transformatie en kwaliteitscontroles te scheiden:

```
queries/
├── df_datakwaliteit/         → Queries voor kwaliteitscontroles
├── df_rawdata_views/         → Views direct op ruwe brontabellen
├── df_staging_tables/        → Staging-tabellen met initiële transformaties
├── df_staging_views/         → Complexere views opgebouwd uit staging tables
```

Daarnaast:
- `setup/`: Bevat standaard functionaliteiten die dmv een operation geimplemeerd kan worden.
  - Elke dataproducer word automatisch een `_lasttransaction` view voor aangemaakt.
- `sources/`: Bevat het overzicht van de gedeclareerde bronnen.

**CRM_ID defineren voor een bron**

* Bij het linken van een CRM_ID aan een bron is de alias gelinkt aan de CRM_ID. Dat doe je bijvoorbeeld zo:
`{ alias: "syntec", crm_id: "982" }`
